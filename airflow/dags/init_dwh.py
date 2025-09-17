from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime
from utils.telegram_alerts import task_success_alert, task_fail_alert


with DAG(
    dag_id="init_dwh",
    start_date=datetime(2025, 9, 1),
    schedule_interval="@once",
    catchup=False,
    tags=["init", "postgres"],
    description="Инициализация схем и таблиц для DWH",
    template_searchpath=["/opt/airflow/sql"],
    on_success_callback = task_success_alert,
    on_failure_callback = task_fail_alert,
) as dag:
    init_schema = PostgresOperator(
        task_id="init_schema",
        postgres_conn_id="postgres_default",
        sql="init_schema.sql"
    )
