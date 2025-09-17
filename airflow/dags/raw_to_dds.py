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
    # Нормализует значения пола пользователя, приводя к стандартным значениям male/female
    if value is None:
        return None
    val = str(value).lower().strip()
    if val in ("male", "female"):
        return val
    return None


def upsert_users_from_raw():
    # Обновляет и добавляет пользователей в DDS из RAW данных, объединяя профили, устройства и местоположения
    with engine.begin() as conn:
        # Получаем пользователей из профилей
        df_profiles = pd.read_sql("SELECT * FROM raw.user_profiles", conn)
        df_devices = pd.read_sql("SELECT * FROM raw.user_devices", conn)
        df_locations = pd.read_sql("SELECT * FROM raw.user_locations", conn)

        # Получаем всех пользователей из событий
        df_events_users = pd.read_sql("SELECT DISTINCT user_id FROM raw.events WHERE user_id IS NOT NULL", conn)

        # Преобразуем user_id к строковому типу
        for df in [df_profiles, df_devices, df_locations, df_events_users]:
            if 'user_id' in df.columns:
                df['user_id'] = df['user_id'].astype(str)

        df_profiles = clean_and_validate(df_profiles, UserProfile, drop_duplicates_by=["user_id"],
                                         not_null_fields=["user_id"])
        df_devices = clean_and_validate(df_devices, UserDevice, drop_duplicates_by=["user_id"],
                                        not_null_fields=["user_id"])
        df_locations = clean_and_validate(df_locations, UserLocation, drop_duplicates_by=["user_id"],
                                          not_null_fields=["user_id"])

        merged = df_profiles.merge(df_devices, on="user_id", how="outer").merge(df_locations, on="user_id", how="outer")
        merged = merged.where(pd.notnull(merged), None)

        existing_users = set(merged['user_id'].unique()) if not merged.empty else set()
        missing_users = set(df_events_users['user_id'].unique()) - existing_users

        if missing_users:
            # Создаем DataFrame для отсутствующих пользователей
            missing_users_df = pd.DataFrame([{
                'user_id': user_id,
                'name': None,
                'email': None,
                'birth_date': None,
                'gender': None,
                'city': None,
                'country': None
            } for user_id in missing_users])

            # Объединяем
            merged = pd.concat([merged, missing_users_df], ignore_index=True)

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
        cur = conn.connection.cursor()
        for _, row in merged.iterrows():
            cur.execute(upsert_sql, (
                row.get("user_id"),
                row.get("name"),
                row.get("email"),
                row.get("birth_date"),
                normalize_gender(row.get("gender")),
                row.get("city"),
                row.get("country")
            ))
        conn.connection.commit()


# ==================== EVENTS ====================


def validate_and_load_events():
    # Загружает и валидирует события из RAW в DDS, обрабатывая типы данных и добавляя недостающие поля
    df = pd.read_sql("SELECT * FROM raw.events", engine)
    if df.empty:
        return 0

    df = df.where(pd.notnull(df), None)
    if "product_id" in df.columns:
        df["product_id"] = df["product_id"].apply(
            lambda x: int(x) if isinstance(x, (int, float)) and not pd.isna(x) else None)
    if "quantity" in df.columns:
        df["quantity"] = df["quantity"].apply(
            lambda x: int(x) if isinstance(x, (int, float)) and not pd.isna(x) else None)

    # добавляем product_name, product_category если их нет
    for col in ["product_name", "product_category"]:
        if col not in df.columns:
            df[col] = None

    df = clean_and_validate(
        df,
        EventModel,
        drop_duplicates_by=["user_id", "event_type", "timestamp"],
        not_null_fields=["user_id", "session_id"]
    )
    if df.empty:
        return 0

    df.to_sql("events", engine, schema="dds", if_exists="append", index=False)
    return len(df)


# ==================== ORDERS ====================


def load_orders_from_events():
    # Извлекает заказы из событий, агрегирует данные и загружает в DDS с созданием минимальных пользователей
    df = pd.read_sql("""
        SELECT 
            e.order_id,
            e.user_id,
            MIN(e.timestamp) as timestamp,
            e.product_id,
            e.quantity,
            e.cost_usd,
            MAX(CASE 
                WHEN e.event_type = 'payment_success' THEN 'success'
                WHEN e.event_type = 'purchase' THEN 'pending'
                ELSE 'pending'
            END) as status
        FROM raw.events e
        WHERE e.order_id IS NOT NULL
        GROUP BY e.order_id, e.user_id, e.product_id, e.quantity, e.cost_usd
    """, engine)

    if df.empty:
        return 0

    # агрегируем заказы
    orders = df.groupby("order_id").agg({
        "user_id": "first",
        "timestamp": "min",
        "cost_usd": lambda s: s.fillna(0).sum()
    }).reset_index().rename(columns={
        "timestamp": "order_ts",
        "cost_usd": "total_usd"
    })

    # позиции заказа
    items = df[["order_id", "product_id", "quantity", "cost_usd"]].dropna(subset=["product_id"])

    # валидация
    orders = clean_and_validate(orders, OrderModel, drop_duplicates_by=["order_id"], not_null_fields=["order_id"])
    items = clean_and_validate(items, OrderItemModel, drop_duplicates_by=["order_id", "product_id"],
                               not_null_fields=["order_id", "product_id"])

    with engine.begin() as conn:
        # Сначала создаем минимальные записи пользователей, которых нет в dds.users
        missing_users = set(orders['user_id'].unique())

        # Проверяем каких пользователей нет в dds.users
        if missing_users:
            missing_users_str = [str(user_id) for user_id in missing_users]

            if missing_users_str:
                placeholders = ','.join(['%s'] * len(missing_users_str))
                query = f"SELECT user_id FROM dds.users WHERE user_id IN ({placeholders})"
                existing_users = pd.read_sql(query, conn, params=missing_users_str)['user_id'].tolist()
            else:
                existing_users = []

            users_to_create = set(missing_users_str) - set(existing_users)

            if users_to_create:
                # Создаем минимальные записи пользователей
                insert_user_sql = """
                INSERT INTO dds.users (user_id, name, email, birth_date, gender, city, country)
                VALUES (%s, NULL, NULL, NULL, NULL, NULL, NULL)
                ON CONFLICT (user_id) DO NOTHING;
                """
                cur = conn.connection.cursor()
                for user_id in users_to_create:
                    cur.execute(insert_user_sql, (user_id,))
                conn.connection.commit()
                logger.info(f"✅ Создано {len(users_to_create)} минимальных записей пользователей")

        # UPSERT для заказов
        upsert_orders = """
        INSERT INTO dds.orders (order_id, user_id, order_ts, total_usd, status)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (order_id) DO UPDATE
        SET user_id = EXCLUDED.user_id,
            order_ts = EXCLUDED.order_ts,
            total_usd = EXCLUDED.total_usd,
            status = EXCLUDED.status;
        """
        cur = conn.connection.cursor()
        for _, row in orders.iterrows():
            # Обработка None значений
            total_usd = row["total_usd"] if pd.notna(row["total_usd"]) else 0.0
            cur.execute(upsert_orders, (
                row["order_id"],
                str(row["user_id"]),
                row["order_ts"],
                float(total_usd),
                row.get("status", "pending")
            ))

        # Вставка для order_items
        insert_items = """
        INSERT INTO dds.order_items (order_id, product_id, quantity, price_usd)
        VALUES (%s, %s, %s, %s)
        """
        for _, row in items.iterrows():
            # Обработка None значений
            product_id = row["product_id"] if pd.notna(row["product_id"]) else None
            quantity = row["quantity"] if pd.notna(row["quantity"]) else 0
            price_usd = row["price_usd"] if pd.notna(row["price_usd"]) else 0.0

            if product_id is not None:
                try:
                    cur.execute(insert_items, (
                        row["order_id"],
                        int(product_id),
                        int(quantity),
                        float(price_usd)
                    ))
                except psycopg2.IntegrityError:
                    # Если запись уже существует, пропускаем или обновляем
                    update_items = """
                    UPDATE dds.order_items 
                    SET quantity = %s, price_usd = %s
                    WHERE order_id = %s AND product_id = %s
                    """
                    cur.execute(update_items, (
                        int(quantity),
                        float(price_usd),
                        row["order_id"],
                        int(product_id)
                    ))

        conn.connection.commit()

    return len(orders) + len(items)


def load_user_devices():
    # Загружает данные об устройствах пользователей из RAW в DDS
    df = pd.read_sql("SELECT user_id, device_type, os, browser FROM raw.user_devices", engine)
    if df.empty:
        return 0
    df = clean_and_validate(df, UserDevice, drop_duplicates_by=["user_id", "device_type", "os", "browser"])
    df.to_sql("user_devices", engine, schema="dds", if_exists="append", index=False)
    return len(df)


def load_user_locations():
    # Загружает географические данные пользователей из RAW в DDS
    df = pd.read_sql("SELECT user_id, city, country, lat, lon FROM raw.user_locations", engine)
    if df.empty:
        return 0
    df = clean_and_validate(df, UserLocation, drop_duplicates_by=["user_id", "city", "country"])
    df.to_sql("user_locations", engine, schema="dds", if_exists="append", index=False)
    return len(df)


def load_sessions():
    # Создает и обновляет сессии пользователей
    with engine.begin() as conn:
        query = """
        INSERT INTO dds.sessions (session_id, user_id, start_ts, end_ts, event_count)
        SELECT
            session_id::uuid,
            user_id::bigint,  
            MIN(timestamp) AS start_ts,
            MAX(timestamp) AS end_ts,
            COUNT(*) AS event_count
        FROM raw.events
        WHERE session_id IS NOT NULL
        GROUP BY session_id, user_id
        ON CONFLICT (session_id) DO UPDATE
        SET end_ts = EXCLUDED.end_ts,
            event_count = EXCLUDED.event_count;
        """
        conn.execute(query)
    logger.info("✅ Сессии обновлены")


def load_products_and_categories():
    #Загружает продукты и категории из событий в DDS, связывая продукты с соответствующими категориями
    with engine.begin() as conn:
        # Сначала загружаем категории
        query_cat = """
        INSERT INTO dds.categories (category_name)
        SELECT DISTINCT product_category
        FROM raw.events
        WHERE product_category IS NOT NULL
        ON CONFLICT (category_name) DO NOTHING;
        """
        conn.execute(query_cat)

        # Получаем уникальные продукты
        df_products = pd.read_sql("""
            SELECT 
                product_id,
                COALESCE(
                    MAX(product_name) FILTER (WHERE product_name IS NOT NULL),
                    'Unknown Product'
                ) as product_name,
                COALESCE(
                    MAX(product_category) FILTER (WHERE product_category IS NOT NULL),
                    'Unknown'
                ) as product_category
            FROM raw.events
            WHERE product_id IS NOT NULL
            GROUP BY product_id
        """, conn)

        # Загружаем продукты по одному
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
                row["product_category"]
            ))
        conn.connection.commit()

    logger.info("✅ Продукты и категории обновлены")


# ==================== CAMPAIGNS ====================
def load_campaigns():
    # Загружает данные о маркетинговых кампаниях из RAW в DDS, создавая таблицу при необходимости
    with engine.begin() as conn:
        # Создаем таблицу для кампаний в DDS
        conn.execute("""
        CREATE TABLE IF NOT EXISTS dds.campaigns (
            campaign_id TEXT PRIMARY KEY,
            campaign_name TEXT,
            channel TEXT
        );
        """)

        # Загрузка данных о кампаниях
        query = """
        INSERT INTO dds.campaigns (campaign_id, campaign_name, channel)
        SELECT DISTINCT campaign_id, campaign_name, channel
        FROM raw.campaign_events
        WHERE campaign_id IS NOT NULL
        ON CONFLICT (campaign_id) DO UPDATE
        SET campaign_name = EXCLUDED.campaign_name,
            channel = EXCLUDED.channel;
        """
        conn.execute(query)
    logger.info("✅ Кампании обновлены")


def update_order_statuses():
    # Обновляет статусы заказов в DDS на основе событий изменения статусов из RAW данных
    with engine.begin() as conn:
        query = """
        UPDATE dds.orders o
        SET status = r.status
        FROM raw.order_status_events r
        WHERE o.order_id = r.order_id::uuid
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
        tags=["dds", "etl", "postgres"]
) as dag:
    t_upsert_users = PythonOperator(task_id="upsert_users", python_callable=upsert_users_from_raw)
    t_load_events = PythonOperator(task_id="load_events", python_callable=validate_and_load_events)
    t_load_orders = PythonOperator(task_id="load_orders", python_callable=load_orders_from_events)
    t_load_sessions = PythonOperator(task_id="load_sessions", python_callable=load_sessions)
    t_load_products = PythonOperator(task_id="load_products", python_callable=load_products_and_categories)
    t_load_campaigns = PythonOperator(task_id="load_campaigns", python_callable=load_campaigns)
    t_update_statuses = PythonOperator(task_id="update_order_statuses", python_callable=update_order_statuses)
    t_load_devices = PythonOperator(task_id="load_user_devices", python_callable=load_user_devices)
    t_load_locations = PythonOperator(task_id="load_user_locations", python_callable=load_user_locations)

    t_upsert_users >> t_load_devices >> t_load_locations >> t_load_events >> [t_load_orders, t_load_sessions, t_load_products, t_load_campaigns] >> t_update_statuses