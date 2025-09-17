import os
import logging
import pandas as pd
import numpy as np
from clickhouse_driver import Client
from airflow.providers.postgres.hooks.postgres import PostgresHook


CLICKHOUSE_CONFIG = {
    "host": "clickhouse",
    "port": 9000,
    "user": os.getenv("CLICKHOUSE_USER", "airflow"),
    "password": os.getenv("CLICKHOUSE_PASSWORD", "airflow"),
    "database": os.getenv("CLICKHOUSE_DB", "data_mart"),
}

# Коннект к Postgres (DDS)
pg_hook = PostgresHook(postgres_conn_id="postgres_default")
engine_pg = pg_hook.get_sqlalchemy_engine()

# Папка с SQL-скриптами
SQL_DIR = "/opt/airflow/sql"


def transfer_table(filename: str, ch_table: str, batch_size: int = 10000):
    # Переносит данные из PostgreSQL в ClickHouse, обеспечивает совместимость типов данных и пакетную загрузку
    try:
        path = os.path.join(SQL_DIR, filename)

        with open(path, "r") as f:
            sql_query = f.read()

        df = pd.read_sql(sql_query, engine_pg)

        if df.empty:
            logging.info(f"⚠️ Нет данных для переноса в {ch_table}")
            return

        # Приведение типов для совместимости с ClickHouse
        if 'user_id' in df.columns:
            df['user_id'] = pd.to_numeric(df['user_id'], errors='coerce').fillna(0).astype('int64')

        # Обработка UUID полей
        uuid_columns = ['session_id', 'order_id']
        for col in uuid_columns:
            if col in df.columns:
                df[col] = df[col].astype(str)

        # Автоматическая обработка всех числовых колонок
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        for col in numeric_cols:
            df[col] = df[col].fillna(0)
            # Замена бесконечных значений
            if df[col].dtype == 'float64':
                df[col] = df[col].replace([np.inf, -np.inf], 0)

        # Обработка строковых колонок
        for col in df.select_dtypes(include=["object"]).columns:
            df[col] = df[col].fillna("")

        # Готовим данные для вставки
        data = [tuple(x) for x in df.to_numpy()]
        columns = list(df.columns)

        client = Client(**CLICKHOUSE_CONFIG)

        # Загружаем
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            insert_query = f"INSERT INTO {ch_table} ({', '.join(columns)}) VALUES"
            client.execute(insert_query, batch)
            logging.info(f"✅ Загружено {len(batch)} строк в {ch_table}")

        logging.info(f"🎉 Полный перенос завершён: {len(data)} строк в {ch_table}")

    except Exception as e:
        logging.error(f"❌ Ошибка при переносе данных в {ch_table}: {e}")
        raise
