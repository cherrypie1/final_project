import json
import psycopg2
from kafka import KafkaConsumer
import pandas as pd
import logging
from airflow.exceptions import AirflowException
from typing import Union
try:
    from typing import get_origin, get_args
except ImportError:
    from typing_extensions import get_origin, get_args

KAFKA_BROKER = "kafka:29092"
TOPIC = "user_events"

# Postgres
DB_CONFIG = {
    "dbname": "dwh",
    "user": "airflow",
    "password": "airflow",
    "host": "postgres",
    "port": 5432
}

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def clean_and_validate(df: pd.DataFrame, model, drop_duplicates_by=None, not_null_fields=None):
    # Очищает и валидирует данные через Pydantic модели, удаляет дубликаты и NULL значения
    valid_records = []
    df = df.where(pd.notnull(df), None)

    for _, row in df.iterrows():
        try:
            row_data = row.to_dict()

            # Обрабатываем None → 0 для числовых типов
            for key, ann in model.__annotations__.items():
                if row_data.get(key) is None:
                    if get_origin(ann) is Union and any(a in (int, float) for a in get_args(ann)):
                        row_data[key] = 0

            rec = model(**row_data)
            valid_records.append(rec.dict())
        except Exception as e:
            logging.warning(f"❌ Отброшена строка {row.to_dict()} → {e}")

    df_clean = pd.DataFrame(valid_records)

    if drop_duplicates_by:
        df_clean = df_clean.drop_duplicates(subset=drop_duplicates_by)

    if not_null_fields:
        df_clean = df_clean.dropna(subset=not_null_fields)

    return df_clean


def consume_and_save(batch_size: int = 100, timeout: int = 30):
    # Читает сообщения из Kafka и кладёт их в соответствующие raw таблицы.
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="airflow_batch_consumer",
        consumer_timeout_ms=timeout * 1000
    )

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    # Подготовленные insert-строки
    insert_events = """
        INSERT INTO raw.events (
            user_id, session_id, event_type,
            product_id, product_name, product_category,
            order_id, quantity,
            city, country, lat, lon,
            cost_usd, cost_eur, cost_rub,
            weather_temp, weather_desc, timestamp
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT DO NOTHING
    """

    insert_profiles = "INSERT INTO raw.user_profiles (user_id, name, email, birth_date, gender) VALUES (%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING"
    insert_devices = "INSERT INTO raw.user_devices (user_id, device_type, os, browser) VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING"
    insert_locations = "INSERT INTO raw.user_locations (user_id, city, country, lat, lon) VALUES (%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING"
    insert_orders = "INSERT INTO raw.orders (order_id, user_id, order_ts, total_usd) VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING"
    insert_order_items = "INSERT INTO raw.order_items (order_id, product_id, quantity, price_usd) VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING"
    insert_campaigns = "INSERT INTO raw.campaign_events (user_id, campaign_id, campaign_name, channel, action, cost, generated_at) VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING"
    insert_order_status = "INSERT INTO raw.order_status_events (order_id, status, generated_at) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING"

    buffers = {
        "events": [],
        "profiles": [],
        "devices": [],
        "locations": [],
        "orders": [],
        "order_items": [],
        "campaigns": [],
        "order_status": []
    }

    try:
        for i, message in enumerate(consumer):
            msg = message.value
            logger.info(f"Received message: {json.dumps(msg, indent=2)}")
            mtype = msg.get("type", "event")

            if mtype == "user_profile":
                buffers["profiles"].append((
                    msg.get("user_id"),
                    msg.get("name"),
                    msg.get("email"),
                    msg.get("birth_date"),
                    msg.get("gender"),
                ))
            elif mtype == "user_device":
                buffers["devices"].append((
                    msg.get("user_id"),
                    msg.get("device_type"),
                    msg.get("os"),
                    msg.get("browser"),
                ))
            elif mtype == "user_location":
                buffers["locations"].append((
                    msg.get("user_id"),
                    msg.get("city"),
                    msg.get("country"),
                    msg.get("lat"),
                    msg.get("lon"),
                ))
            elif mtype == "order":
                buffers["orders"].append((
                    msg.get("order_id"),
                    msg.get("user_id"),
                    msg.get("order_ts"),
                    msg.get("total_usd"),
                ))
            elif mtype == "order_item":
                buffers["order_items"].append((
                    msg.get("order_id"),
                    msg.get("product_id"),
                    msg.get("quantity"),
                    msg.get("price_usd"),
                ))
            elif mtype == "campaign_event":
                buffers["campaigns"].append((
                    msg.get("user_id"),
                    msg.get("campaign_id"),
                    msg.get("campaign_name"),
                    msg.get("channel"),
                    msg.get("action"),
                    msg.get("cost"),
                    msg.get("generated_at"),
                ))
            elif mtype == "order_status":
                buffers["order_status"].append((
                    msg.get("order_id"),
                    msg.get("status"),
                    msg.get("generated_at"),
                ))
            elif mtype in ("session_start", "session_end", "signup", "churn"):
                buffers["events"].append((
                    msg.get("user_id"),
                    msg.get("session_id"),
                    mtype,
                    msg.get("product_id"),
                    msg.get("product_name"),
                    msg.get("product_category"),
                    msg.get("order_id"),
                    msg.get("quantity"),
                    msg.get("city"),
                    msg.get("country"),
                    msg.get("lat"),
                    msg.get("lon"),
                    msg.get("cost_usd"),
                    msg.get("cost_eur"),
                    msg.get("cost_rub"),
                    msg.get("weather_temp"),
                    msg.get("weather_desc"),
                    msg.get("timestamp"),
                ))
            else:
                buffers["events"].append((
                    msg.get("user_id"),
                    msg.get("session_id"),
                    msg.get("event_type"),
                    msg.get("product_id"),
                    msg.get("product_name"),
                    msg.get("product_category"),
                    msg.get("order_id"),
                    msg.get("quantity"),
                    msg.get("city"),
                    msg.get("country"),
                    msg.get("lat"),
                    msg.get("lon"),
                    msg.get("cost_usd"),
                    msg.get("cost_eur"),
                    msg.get("cost_rub"),
                    msg.get("weather_temp"),
                    msg.get("weather_desc"),
                    msg.get("timestamp"),
                ))

            # flush по batch_size
            if (i + 1) % batch_size == 0:
                _flush_buffers(cur, buffers, insert_events, insert_profiles,
                               insert_devices, insert_locations, insert_orders,
                               insert_order_items, insert_campaigns, insert_order_status)
                conn.commit()
                for k in buffers: buffers[k].clear()

        # final flush
        _flush_buffers(cur, buffers, insert_events, insert_profiles,
                       insert_devices, insert_locations, insert_orders,
                       insert_order_items, insert_campaigns, insert_order_status)
        conn.commit()
        logger.info("[SUCCESS] Consumer finished and flushed buffers")

    except Exception as e:
        conn.rollback()
        logger.exception("Consumer failed")
        raise AirflowException(f"Consumer failed: {e}")
    finally:
        cur.close()
        conn.close()
        consumer.close()


def _flush_buffers(cur, buffers, insert_events, insert_profiles,
                   insert_devices, insert_locations, insert_orders, insert_order_items, insert_campaigns, insert_order_status):
    if buffers["events"]:
        cur.executemany(insert_events, buffers["events"])
    if buffers["profiles"]:
        cur.executemany(insert_profiles, buffers["profiles"])
    if buffers["devices"]:
        cur.executemany(insert_devices, buffers["devices"])
    if buffers["locations"]:
        cur.executemany(insert_locations, buffers["locations"])
    if buffers["orders"]:
        cur.executemany(insert_orders, buffers["orders"])
    if buffers["order_items"]:
        cur.executemany(insert_order_items, buffers["order_items"])
    if buffers["campaigns"]:
        cur.executemany(insert_campaigns, buffers["campaigns"])
    if buffers["order_status"]:
        cur.executemany(insert_order_status, buffers["order_status"])


def check_kafka_message():
    # Проверяет, есть ли хотя бы одно сообщение в Kafka
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=5000
    )
    try:
        for _ in consumer:
            return True
        return False
    finally:
        consumer.close()