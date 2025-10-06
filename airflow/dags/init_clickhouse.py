from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from utils.telegram_alerts import task_success_alert, task_fail_alert

with DAG(
        dag_id="init_clickhouse",
        start_date=datetime(2025, 9, 1),
        schedule_interval="@once",
        catchup=False,
        tags=["init", "clickhouse"],
        description="Инициализация базы данных и таблиц в ClickHouse",
        on_success_callback=task_success_alert,
        on_failure_callback=task_fail_alert,
) as dag:
    init_clickhouse = BashOperator(
        task_id="init_clickhouse",
        bash_command="docker exec -i clickhouse clickhouse-client -h localhost --port 9000 -u airflow --password airflow --multiquery < /opt/airflow/sql/clickhouse_init.sql",
    )