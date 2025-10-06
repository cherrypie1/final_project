import os
import logging
import pandas as pd
import numpy as np
import datetime
from clickhouse_driver import Client
from sqlalchemy import create_engine
from airflow.providers.postgres.hooks.postgres import PostgresHook

CLICKHOUSE_CONFIG = {
    "host": "clickhouse",
    "port": 9000,
    "user": os.getenv("CLICKHOUSE_USER", "airflow"),
    "password": os.getenv("CLICKHOUSE_PASSWORD", "airflow"),
    "database": os.getenv("CLICKHOUSE_DB", "data_mart"),
}

SQL_DIR = "/opt/airflow/sql"

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


DATE_ONLY_COLUMNS = ['event_date', 'last_session_date', 'first_session_date', 'start_date', 'end_date']
DATETIME_COLUMNS = ['start_time', 'end_time', 'start_ts', 'end_ts', 'timestamp']
ALL_DATE_COLUMNS_TO_HANDLE = DATE_ONLY_COLUMNS + DATETIME_COLUMNS


INTEGER_COLUMNS_TO_FIX = [
    'product_id', 'total_quantity', 'orders_count', 'unique_customers',
    'total_orders', 'total_sessions', 'new_users', 'successful_orders',
    'total_users', 'total_events', 'sessions_count', 'events_count',
    'page_views_count', 'duration_seconds', 'conversion_flag', 'user_id',
    'days_since_last_activity', 'active_users_count', 'returning_users', 
    'churned_users', 'unique_users'
]


def validate_data_quality(df, table_name):
    """Валидация качества данных перед загрузкой"""
    
    original_count = len(df)
    
    # Проверка аномальных цен
    if 'total_revenue' in df.columns:
        high_revenue = df[df['total_revenue'] > 10000]
        if len(high_revenue) > 0:
            logger.warning(f"Обнаружены аномальные цены в {table_name}: {len(high_revenue)} записей")
            # Обрезаем значения вместо удаления
            df['total_revenue'] = df['total_revenue'].clip(upper=10000)
    
    # Проверка конверсии
    if 'conversion_rate' in df.columns:
        invalid_conversion = df[
            (df['conversion_rate'] < 0) | (df['conversion_rate'] > 1)
        ]
        if len(invalid_conversion) > 0:
            logger.warning(f"Некорректная конверсия в {table_name}: {len(invalid_conversion)} записей")
            df['conversion_rate'] = df['conversion_rate'].clip(0, 1)
    
    # Проверка ROI
    if 'roi' in df.columns:
        extreme_roi = df[df['roi'].abs() > 100]  # ROI > 10000% или < -10000%
        if len(extreme_roi) > 0:
            logger.warning(f" Экстремальный ROI в {table_name}: {len(extreme_roi)} записей")
            df['roi'] = df['roi'].clip(-100, 100)
    
    # Проверка avg_order_value
    if 'avg_order_value' in df.columns:
        high_avg = df[df['avg_order_value'] > 10000]
        if len(high_avg) > 0:
            logger.warning(f"Аномальный avg_order_value в {table_name}: {len(high_avg)} записей")
            df['avg_order_value'] = df['avg_order_value'].clip(upper=10000)
    
    # Проверка на отрицательные значения в количественных метриках
    quantity_columns = ['total_quantity', 'orders_count', 'total_orders', 'total_sessions', 'successful_orders']
    for col in quantity_columns:
        if col in df.columns:
            negative_values = df[df[col] < 0]
            if len(negative_values) > 0:
                logger.warning(f"Отрицательные значения в {col}: {len(negative_values)} записей")
                df[col] = df[col].clip(lower=0)
    
    logger.info(f"✅ Валидация данных завершена. Обработано {original_count} записей")
    return df


def transfer_table(pg_query: str, ch_table: str, date_field: str = "event_date", days_to_keep: int = 30):
    """
    функция для переноса данных из PostgreSQL → ClickHouse.
    """
    logger.info(f"🔹 Старт переноса данных в `{ch_table}`")

    pg_hook = PostgresHook(postgres_conn_id="postgres_default")
    pg_engine = pg_hook.get_sqlalchemy_engine()
    client = Client(**CLICKHOUSE_CONFIG)

    try:
        # Очистка старых данных в ClickHouse
        if date_field:
            try:
                logger.info(f"Очистка данных в {ch_table} за последние {days_to_keep} дней...")
                client.execute(f"""
                    ALTER TABLE {ch_table} DELETE WHERE {date_field} >= today() - {days_to_keep};
                """)
                client.execute(f"OPTIMIZE TABLE {ch_table} FINAL")
                logger.info(f"Очистка {ch_table} завершена")
            except Exception as e:
                logger.warning(f"Не удалось очистить {ch_table}: {e}")

        # Загрузка данных из PostgreSQL
        logger.info("Загрузка данных из PostgreSQL...")
        df = pd.read_sql(pg_query, pg_engine)
        logger.info(f"Получено {len(df)} строк")

        if df.empty:
            logger.warning(f"Нет данных для загрузки в {ch_table}")
            return

        # Очистка данных
        before_dedup = len(df)
        df = df.drop_duplicates()
        after_dedup = len(df)
        if after_dedup < before_dedup:
            logger.info(f"Удалено {before_dedup - after_dedup} дубликатов")

        df = validate_data_quality(df, ch_table)

        # Нормализация основного поля даты/времени.
        if date_field in df.columns:
            df[date_field] = pd.to_datetime(df[date_field])

            if date_field in DATE_ONLY_COLUMNS:
                # Если это поле Date, отбрасываем время
                df[date_field] = df[date_field].dt.normalize()

            df = df[df[date_field].notnull()]

        # Очистка строковых полей
        for col in df.select_dtypes(include="object").columns:
            # Пропускаем даты/время, чтобы не превращать их в строки
            if col in ALL_DATE_COLUMNS_TO_HANDLE:
                continue

            df[col] = df[col].fillna("").astype(str)

        # Фильтрация географических данных
        if "country" in df.columns:
            before_country = len(df)
            df = df[~df["country"].isin(["Unknown", "Antarctica", ""])]
            after_country = len(df)
            if after_country < before_country:
                logger.info(f"🌍 Отфильтровано {before_country - after_country} записей с Unknown/Antarctica странами")
                
        if "city" in df.columns:
            before_city = len(df)
            df = df[~df["city"].isin(["Unknown", "Antarctica", ""])]
            after_city = len(df)
            if after_city < before_city:
                logger.info(f"🏙️ Отфильтровано {before_city - after_city} записей с Unknown/Antarctica городами")

        logger.info(f"✅ После очистки осталось {len(df)} строк")

        for col in df.select_dtypes(include=['float', 'int']).columns:
            df[col] = df[col].apply(lambda x: x.item() if isinstance(x, np.generic) else x)
        logger.info("Преобразованы числовые столбцы из NumPy-скаляров в Python-типы.")

        for col in df.select_dtypes(include=[object, 'object']).columns:
            is_converted = False
            try:
                df[col] = df[col].apply(lambda x: x.tolist() if isinstance(x, np.ndarray) else x)
                is_converted = True
            except Exception:
                pass

            if is_converted:
                logger.info(f"Преобразован столбец {col} в Python list.")

        for col in INTEGER_COLUMNS_TO_FIX:
            if col in df.columns and pd.api.types.is_float_dtype(df[col]):
                try:
                    df[col] = df[col].astype(pd.Int64Dtype()).apply(lambda x: int(x) if pd.notna(x) else None)
                    logger.info(f"Исправлено (INT): Столбец {col} преобразован из float в int.")
                except Exception as e:
                    logger.warning(f"Не удалось принудительно преобразовать {col} в int: {e}. Пропускаем приведение типа.")

        for col in ALL_DATE_COLUMNS_TO_HANDLE:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')

                if col in DATE_ONLY_COLUMNS:

                    df[col] = df[col].dt.normalize().apply(
                        lambda x: x.to_pydatetime().date() if pd.notna(x) else None
                    )
                    logger.info(f"Финал (DATE): Столбец {col} преобразован в Python date.")

                elif col in DATETIME_COLUMNS:

                    df[col] = df[col].where(pd.notna(df[col]), None)

                    def to_utc_datetime_or_none(dt):
                        if dt is None:
                            return None

                        # Преобразование Pandas Timestamp в Python datetime.
                        py_dt = dt.to_pydatetime()
                        if py_dt.tzinfo is None or py_dt.tzinfo.utcoffset(py_dt) is None:
                            return py_dt.replace(tzinfo=datetime.timezone.utc)
                        return py_dt.astimezone(datetime.timezone.utc)

                    df[col] = df[col].apply(to_utc_datetime_or_none)

                    logger.info(f"Финал (DATETIME - TZ-AWARE): Столбец {col} преобразован в Python datetime.datetime (UTC).")

        # Очистка таблицы перед вставкой
        try:
            client.execute(f"TRUNCATE TABLE {ch_table}")
            logger.info(f"Таблица {ch_table} очищена перед загрузкой.")
        except Exception as e:
            logger.warning(f"Не удалось очистить {ch_table}: {e}")

        # Запись в ClickHouse
        data_to_insert = df.to_dict('records')

        if not data_to_insert:
            logger.warning(f"Нет данных для вставки в {ch_table} после преобразований")
            return

        # Вставка построчно, используя типы Python
        client.execute(
            f"INSERT INTO {ch_table} ({','.join(df.columns)}) VALUES",
            data_to_insert,
            types_check=True
        )
        logger.info(f"Успешно вставлено {len(data_to_insert)} строк в {ch_table}")

    except Exception as e:
        logger.error(f"❌ Ошибка при переносе данных в {ch_table}: {e}", exc_info=True)

        raise
    finally:
        pg_engine.dispose()
        logger.info(f"✅ Завершено обновление таблицы {ch_table}\n")

