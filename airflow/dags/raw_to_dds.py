from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
import pandas as pd
from utils.telegram_alerts import task_success_alert, task_fail_alert
from utils.dags_utils import clean_and_validate
from utils.pyndatic_schemas import (
    UserProfile, UserDevice, UserLocation,
    EventModel, OrderModel, OrderItemModel
)
from sqlalchemy import create_engine
import psycopg2

engine = create_engine("postgresql+psycopg2://airflow:airflow@postgres:5432/dwh")

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_success_callback": task_success_alert,
    "on_failure_callback": task_fail_alert,
}


# ==================== USERS ====================
def normalize_gender(value):
    if value is None:
        return None
    val = str(value).lower().strip()
    if val in ("male", "female"):
        return val
    return None


def upsert_users_from_raw():
    with engine.begin() as conn:
        # Читаем данные из raw таблиц
        df_profiles = pd.read_sql("SELECT * FROM raw.user_profiles", conn)
        df_events_users = pd.read_sql(
            "SELECT DISTINCT user_id FROM raw.events WHERE user_id IS NOT NULL", conn
        )

        # Обрабатываем профили
        valid_dfs = []
        if "user_id" in df_profiles.columns and not df_profiles.empty:
            df_profiles["user_id"] = pd.to_numeric(df_profiles["user_id"], errors="coerce").astype("Int64")
            df_profiles = df_profiles.dropna(subset=["user_id"])
            valid_dfs.append(df_profiles)
            logger.info("✅ Обработан df_profiles: %d записей", len(df_profiles))

        # Объединяем данные
        if valid_dfs:
            merged = valid_dfs[0]
            merged = merged.where(pd.notnull(merged), None)
        else:
            merged = pd.DataFrame()


        if "user_id" in df_events_users.columns and not df_events_users.empty:
            df_events_users["user_id"] = pd.to_numeric(df_events_users["user_id"], errors="coerce").astype("Int64")
            df_events_users = df_events_users.dropna(subset=["user_id"])

            existing_users = set(merged["user_id"].unique()) if not merged.empty else set()
            missing_users = set(df_events_users["user_id"].unique()) - existing_users

            if missing_users:

                user_ids_str = ",".join([f"'{str(uid)}'" for uid in missing_users])

                active_users_check = pd.read_sql(f"""
                    SELECT DISTINCT user_id 
                    FROM raw.events 
                    WHERE user_id IN ({user_ids_str})
                    AND event_type NOT IN ('session_start', 'session_end', 'signup')
                """, conn)

                # Конвертируем user_id обратно в числовой тип
                active_users_check["user_id"] = pd.to_numeric(active_users_check["user_id"], errors="coerce").astype(
                    "Int64")
                active_users_check = active_users_check.dropna(subset=["user_id"])

                really_active_users = set(active_users_check["user_id"].unique())
                users_to_add = missing_users & really_active_users

                if users_to_add:
                    missing_users_df = pd.DataFrame([{
                        "user_id": user_id,
                        "name": None,
                        "email": None,
                        "birth_date": None,
                        "gender": None,
                        "city": None,
                        "country": None
                    } for user_id in users_to_add])

                    if merged.empty:
                        merged = missing_users_df
                    else:
                        merged = pd.concat([merged, missing_users_df], ignore_index=True)

                    logger.info("✅ Добавлено %d реальных пользователей из событий", len(users_to_add))

        if merged.empty:
            logger.warning(" Нет данных для вставки в dds.users")
            return 0

        # ИСПРАВЛЕНИЕ: Правильная очистка с учетом типов данных
        cleanup_sql = """
        DELETE FROM dds.users 
        WHERE user_id NOT IN (
            SELECT DISTINCT user_id::bigint FROM raw.user_profiles WHERE user_id IS NOT NULL
            UNION
            SELECT DISTINCT user_id::bigint FROM raw.events 
            WHERE user_id IS NOT NULL 
            AND event_type NOT IN ('session_start', 'session_end', 'signup')
        )
        """
        cur = conn.connection.cursor()
        cur.execute(cleanup_sql)
        logger.info("Очистка лишних пользователей выполнена")

        # Вставляем/обновляем данные
        upsert_sql = """
            INSERT INTO dds.users (user_id, name, email, birth_date, gender, city, country)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (user_id) DO UPDATE
              SET name = COALESCE(EXCLUDED.name, dds.users.name),
                  email = COALESCE(EXCLUDED.email, dds.users.email),
                  birth_date = COALESCE(EXCLUDED.birth_date, dds.users.birth_date),
                  gender = COALESCE(EXCLUDED.gender, dds.users.gender),
                  city = COALESCE(EXCLUDED.city, dds.users.city),
                  country = COALESCE(EXCLUDED.country, dds.users.country)
        """

        inserted_count = 0
        for _, row in merged.iterrows():
            try:
                cur.execute(upsert_sql, (
                    row.get("user_id"),
                    row.get("name"),
                    row.get("email"),
                    row.get("birth_date"),
                    normalize_gender(row.get("gender")),
                    row.get("city"),
                    row.get("country"),
                ))
                inserted_count += 1
            except Exception as e:
                logger.error("❌ Ошибка при вставке пользователя %s: %s", row.get("user_id"), e)

        conn.connection.commit()
        logger.info("✅ Вставлено/обновлено %d пользователей", inserted_count)
        return inserted_count


# ==================== EVENTS ====================
def validate_and_load_events():
    df = pd.read_sql("SELECT * FROM raw.events", engine)
    if df.empty:
        logger.info(" Нет новых событий в raw.events")
        return 0

    df = df.where(pd.notnull(df), None)

    # Приводим timestamp к datetime
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

    # Чистим числовые поля
    for col in ["product_id", "quantity"]:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: int(x) if pd.notna(x) else None)

    # Фильтруем только допустимые события
    allowed_event_types = {
        "session_start", "session_end", "purchase",
        "payment_success", "payment_failed",
        "signup", "churn",
        "view_product", "add_to_cart", "view_page"
    }
    df = df[df["event_type"].isin(allowed_event_types)]

    # signup/churn могут быть без session_id
    drop_fields = ["user_id"] if df["event_type"].isin(["signup", "churn"]).any() else ["user_id", "session_id"]

    df = clean_and_validate(
        df,
        EventModel,
        drop_duplicates_by=["user_id", "event_type", "timestamp"],
        not_null_fields=drop_fields
    )

    if df.empty:
        logger.info(" После очистки событий нет данных для загрузки в DDS")
        return 0

    df.to_sql("events", engine, schema="dds", if_exists="append", index=False, method='multi', chunksize=1000)

    logger.info(f"✅ Загружено {len(df)} событий в dds.events")
    return len(df)


# ==================== ORDERS ====================
def load_orders_from_events():
    logger.info("Начинаем загрузку заказов из raw.orders...")

    # ИСПРАВЛЕНИЕ: Используем ТОЛЬКО raw.orders как источник
    df_orders = pd.read_sql("""
        SELECT DISTINCT
            o.order_id,
            o.user_id,
            o.order_ts,
            o.total_usd,
            COALESCE(ose.status, 'pending') as status
        FROM raw.orders o
        LEFT JOIN (
            SELECT DISTINCT ON (order_id) order_id, status
            FROM raw.order_status_events 
            WHERE order_id IS NOT NULL
            ORDER BY order_id, generated_at DESC
        ) ose ON o.order_id = ose.order_id
        WHERE o.order_id IS NOT NULL
    """, engine)

    logger.info(f"Найдено {len(df_orders)} заказов в raw.orders")

    # Позиции заказов из raw.order_items
    df_order_items = pd.read_sql("""
        SELECT 
            order_id,
            product_id,
            quantity,
            price_usd
        FROM raw.order_items 
        WHERE order_id IS NOT NULL 
          AND product_id IS NOT NULL
          AND price_usd IS NOT NULL
    """, engine)

    logger.info(f"📋 Найдено {len(df_order_items)} позиций в raw.order_items")

    if df_orders.empty:
        logger.info("Нет данных о заказах")
        return 0

    with engine.begin() as conn:
        cur = conn.connection.cursor()

        cleanup_sql = """
        DELETE FROM dds.orders 
        WHERE order_id::text NOT IN (
            SELECT DISTINCT order_id FROM raw.orders WHERE order_id IS NOT NULL
        )
        """
        result = cur.execute(cleanup_sql)
        logger.info(f" Очищено заказов из других источников")

        # UPSERT заказов из raw.orders
        upsert_orders = """
        INSERT INTO dds.orders (order_id, user_id, order_ts, total_usd, status)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (order_id) DO UPDATE
        SET user_id = EXCLUDED.user_id,
            order_ts = EXCLUDED.order_ts,
            total_usd = EXCLUDED.total_usd,
            status = EXCLUDED.status;
        """

        orders_count = 0
        for _, row in df_orders.iterrows():
            try:
                # Проверяем, что заказ из raw.orders
                if pd.isna(row["order_id"]) or pd.isna(row["user_id"]):
                    continue

                cur.execute(upsert_orders, (
                    row["order_id"],
                    str(row["user_id"]),
                    row["order_ts"],
                    float(row["total_usd"]) if pd.notna(row["total_usd"]) else 0.0,
                    row["status"],
                ))
                orders_count += 1
            except Exception as e:
                logger.error(f"❌ Ошибка при вставке заказа {row['order_id']}: {e}")

        # Вставка позиций заказа
        items_count = 0
        insert_items = """
        INSERT INTO dds.order_items (order_id, product_id, quantity, price_usd)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (order_id, product_id) DO UPDATE
        SET quantity = EXCLUDED.quantity,
            price_usd = EXCLUDED.price_usd;
        """

        for _, row in df_order_items.iterrows():
            try:
                cur.execute(insert_items, (
                    row["order_id"],
                    int(row["product_id"]),
                    int(row["quantity"]) if pd.notna(row["quantity"]) else 1,
                    float(row["price_usd"]),
                ))
                items_count += 1
            except Exception as e:
                logger.error(f"❌ Ошибка при вставке позиции заказа {row['order_id']}: {e}")

        conn.connection.commit()

    logger.info(f"✅ Загружено {orders_count} заказов и {items_count} позиций")
    return orders_count + items_count


# ==================== DEVICES / LOCATIONS ====================
def load_user_devices():
    df = pd.read_sql("SELECT user_id, device_type, os, browser FROM raw.user_devices", engine)
    if df.empty:
        return 0
    df = clean_and_validate(df, UserDevice, drop_duplicates_by=["user_id", "device_type", "os", "browser"])
    df.to_sql(
        "user_devices",
        engine,
        schema="dds",
        if_exists="append",
        index=False,
        method='multi',
        chunksize=1000
    )
    logger.info(f"✅ Загружено {len(df)} устройств пользователей в dds.user_devices")

    return len(df)


def load_user_locations():
    df = pd.read_sql("SELECT user_id, city, country, lat, lon FROM raw.user_locations", engine)
    if df.empty:
        return 0
    df = clean_and_validate(df, UserLocation, drop_duplicates_by=["user_id", "city", "country"])

    if not df.empty:
        with engine.begin() as conn:
            user_ids = df["user_id"].dropna().unique().tolist()
            if user_ids:
                placeholders = ",".join(["%s"] * len(user_ids))
                query = f"SELECT user_id FROM dds.users WHERE user_id IN ({placeholders})"
                existing_users = pd.read_sql(query, conn, params=user_ids)["user_id"].tolist()
                missing_users = set(user_ids) - set(existing_users)

                if missing_users:
                    insert_sql = """
                    INSERT INTO dds.users (user_id, name, email, birth_date, gender, city, country)
                    VALUES (%s, NULL, NULL, NULL, NULL, NULL, NULL)
                    ON CONFLICT (user_id) DO NOTHING;
                    """
                    cur = conn.connection.cursor()
                    for uid in missing_users:
                        cur.execute(insert_sql, (int(uid),))
                    conn.connection.commit()
                    logger.info(f"✅ Созданы {len(missing_users)} недостающие пользователи")

    # теперь можно грузить локации
    df.to_sql(
        "user_locations",
        engine,
        schema="dds",
        if_exists="append",
        index=False,
        method='multi',
        chunksize=1000
    )
    logger.info(f"✅ Загружено {len(df)} локаций пользователей в dds.user_locations")
    return len(df)


# ==================== SESSIONS ====================
def load_sessions():
    with engine.begin() as conn:
        # 1. Сначала создаем недостающих пользователей из событий с правильными типами
        create_missing_users_query = """
            INSERT INTO dds.users (user_id, name, email, birth_date, gender, city, country)
            SELECT DISTINCT 
                e.user_id::bigint,
                NULL::text as name,
                NULL::text as email,
                NULL::date as birth_date,
                NULL::text as gender,
                NULL::text as city,
                NULL::text as country
            FROM raw.events e
            LEFT JOIN dds.users u ON e.user_id::bigint = u.user_id
            WHERE e.user_id IS NOT NULL 
              AND u.user_id IS NULL
            ON CONFLICT (user_id) DO NOTHING;
            """
        result_users = conn.execute(create_missing_users_query)
        logger.info(f"✅ Создано {result_users.rowcount} недостающих пользователей для сессий")
            
        # 2. Загружаем сессии только для существующих пользователей
        query = """
        INSERT INTO dds.sessions (session_id, user_id, start_ts, end_ts, event_count)
        SELECT
            session_id::uuid,
            user_id::bigint,
            MIN(timestamp) FILTER (WHERE event_type = 'session_start') AS start_ts,
            MAX(timestamp) FILTER (WHERE event_type = 'session_end')   AS end_ts,
            COUNT(*) AS event_count
        FROM raw.events
        WHERE session_id IS NOT NULL 
          AND user_id IS NOT NULL
          AND user_id::bigint IN (SELECT user_id FROM dds.users)
        GROUP BY session_id, user_id
        ON CONFLICT (session_id) DO UPDATE
        SET start_ts   = EXCLUDED.start_ts,
            end_ts     = EXCLUDED.end_ts,
            event_count = EXCLUDED.event_count;
        """
        result = conn.execute(query)
        logger.info(f"✅ Сессии обновлены: {result.rowcount} записей")


# ==================== PRODUCTS ====================
def load_products_and_categories():
    with engine.begin() as conn:
        query_cat = """
        INSERT INTO dds.categories (category_name)
        SELECT DISTINCT product_category
        FROM raw.events
        WHERE product_category IS NOT NULL
        ON CONFLICT (category_name) DO NOTHING;
        """
        conn.execute(query_cat)

        df_products = pd.read_sql("""
            SELECT
                product_id,
                COALESCE(MAX(product_name) FILTER (WHERE product_name IS NOT NULL), 'Unknown Product') as product_name,
                COALESCE(MAX(product_category) FILTER (WHERE product_category IS NOT NULL), 'Unknown') as product_category
            FROM raw.events
            WHERE product_id IS NOT NULL
            GROUP BY product_id
        """, conn)

        upsert_sql = """
        INSERT INTO dds.products (product_id, product_name, category_id)
        SELECT %s, %s, c.category_id
        FROM dds.categories c
        WHERE c.category_name = %s
        ON CONFLICT (product_id) DO UPDATE
        SET product_name = EXCLUDED.product_name,
            category_id = EXCLUDED.category_id;
        """

        cur = conn.connection.cursor()
        for _, row in df_products.iterrows():
            cur.execute(upsert_sql, (
                row["product_id"],
                row["product_name"],
                row["product_category"],
            ))
        conn.connection.commit()
    logger.info("✅ Продукты и категории обновлены")


# ==================== CAMPAIGNS ====================
def load_campaigns():
    with engine.begin() as conn:
        conn.execute(""" 
        CREATE TABLE IF NOT EXISTS dds.campaigns ( 
        campaign_id TEXT PRIMARY KEY, 
        campaign_name TEXT, 
        channel TEXT ); """)

        query = """ 
        INSERT INTO dds.campaigns (campaign_id, campaign_name, channel) 
        SELECT DISTINCT ON (campaign_id) campaign_id, campaign_name, channel 
        FROM raw.campaign_events 
        WHERE campaign_id IS NOT NULL 
        ORDER BY campaign_id, generated_at DESC 
        ON CONFLICT (campaign_id) DO UPDATE 
        SET campaign_name = EXCLUDED.campaign_name, channel = EXCLUDED.channel; """
        conn.execute(query)
    logger.info("✅ Кампании обновлены")


def load_campaign_events():
    with engine.begin() as conn:
        query = """
        INSERT INTO dds.campaign_events (user_id, campaign_id, action, cost, generated_at)
        SELECT
            user_id::bigint,
            campaign_id,
            action,
            CASE 
                WHEN cost IS NOT NULL THEN cost
                WHEN action = 'view' THEN 0.1
                WHEN action = 'click' THEN 0.3
                WHEN action = 'purchase' THEN 1.0
                ELSE 0.05
            END AS cost,
            generated_at
        FROM raw.campaign_events
        WHERE campaign_id IS NOT NULL
        ON CONFLICT DO NOTHING;
        """
        conn.execute(query)
    logger.info("✅ События кампаний обновлены (cost нормализован)")


# ==================== ORDER STATUSES ====================
def update_order_statuses():
    with engine.begin() as conn:
        query = """
            UPDATE dds.orders o
            SET status = r.status
            FROM raw.order_status_events r
            WHERE o.order_id::text = r.order_id::text
              AND (
                  o.status IS NULL
                  OR o.status = 'pending'  
                  OR r.status IN ('failed', 'cancelled')
              );
        """
        result = conn.execute(query)
        logger.info(f"✅ Обновлено статусов заказов: {result.rowcount}")


# ==================== DAG ====================
with DAG(
        dag_id="raw_to_dds_enhanced",
        default_args=default_args,
        description="RAW -> DDS с валидацией, нормализацией и UPSERT",
        schedule_interval=timedelta(hours=1),
        start_date=datetime(2025, 8, 30),
        catchup=False,
        tags=["dds", "etl", "postgres"],
) as dag:
    t_upsert_users = PythonOperator(task_id="upsert_users", python_callable=upsert_users_from_raw)
    t_load_events = PythonOperator(task_id="load_events", python_callable=validate_and_load_events)
    t_load_orders = PythonOperator(task_id="load_orders", python_callable=load_orders_from_events)
    t_load_sessions = PythonOperator(task_id="load_sessions", python_callable=load_sessions)
    t_load_products = PythonOperator(task_id="load_products", python_callable=load_products_and_categories)
    t_load_campaigns = PythonOperator(task_id="load_campaigns", python_callable=load_campaigns)
    t_load_campaign_events = PythonOperator(task_id="load_campaign_events", python_callable=load_campaign_events)
    t_update_statuses = PythonOperator(task_id="update_order_statuses", python_callable=update_order_statuses)
    t_load_devices = PythonOperator(task_id="load_user_devices", python_callable=load_user_devices)
    t_load_locations = PythonOperator(task_id="load_user_locations", python_callable=load_user_locations)

    # порядок выполнения
    t_upsert_users
    t_load_events
    t_load_products
    t_upsert_users >> t_load_devices
    t_upsert_users >> t_load_locations
    t_load_events >> t_load_sessions
    t_load_events >> t_load_orders
    t_load_orders >> t_update_statuses
    t_load_campaigns >> t_load_campaign_events
