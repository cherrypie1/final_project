from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from datetime import datetime, timedelta
from utils.dags_utils import consume_and_save, check_kafka_message
from utils.telegram_alerts import task_success_alert, task_fail_alert


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_success_callback": task_success_alert,
    "on_failure_callback": task_fail_alert,
}

with DAG(
    dag_id="kafka_to_postgres",
    default_args=default_args,
    description="Загрузка сообщений из Kafka в Postgres (raw слой)",
    schedule_interval=timedelta(minutes=30),
    start_date=datetime(2025, 8, 30),
    catchup=False,
    tags=["postgres", "kafka", "raw"],
    template_searchpath=["/opt/airflow/sql"]
) as dag:

    wait_for_kafka = PythonSensor(
        task_id="wait_for_kafka",
        python_callable=check_kafka_message,
        poke_interval=10,
        timeout=60 * 5,
        mode="poke"
    )

    consume_task = PythonOperator(
        task_id="consume_and_save_batch",
        python_callable=consume_and_save,
        op_kwargs={"batch_size": 50, "timeout": 30},
        execution_timeout=timedelta(minutes=5)
    )

    wait_for_kafka >> consume_task
