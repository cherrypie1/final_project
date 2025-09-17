from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
from utils.telegram_alerts import task_success_alert, task_fail_alert
from utils.generator import produce_batch, KAFKA_BROKER

logger = logging.getLogger(__name__)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "on_success_callback": task_success_alert,
    "on_failure_callback": task_fail_alert
}

with DAG(
    dag_id="generate_events_minutely",
    default_args=default_args,
    description="Генерация событий в Kafka (каждую минуту)",
    schedule_interval=timedelta(minutes=1),
    start_date=datetime(2025, 9, 1),
    catchup=False,
    max_active_runs=1,
    tags=["generator", "kafka"]
) as dag:

    def produce_task(**kwargs):
        # отправляем 1 пользователя + 2 события
        try:
            sent = produce_batch(send_count_users=1, events_per_user=2, broker=KAFKA_BROKER)
            logger.info(f"Produced {sent} messages to Kafka")
            return sent
        except Exception as e:
            logger.exception("Producer failed")
            raise

    produce = PythonOperator(
        task_id="produce_entities_and_events",
        python_callable=produce_task,
        provide_context=True
    )

    produce