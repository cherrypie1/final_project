from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from utils.telegram_alerts import task_success_alert, task_fail_alert
from utils.ch_utils import transfer_table

# Базовые настройки DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 9, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_success_callback": task_success_alert,
    "on_failure_callback": task_fail_alert,
}

SQL_PATH = "/opt/airflow/sql"


def run_transfer(sql_file: str, table_name: str, date_field: str = "event_date"):
    """
    Вспомогательная функция-обёртка для PythonOperator:
    читает SQL из файла и передаёт его в transfer_table().
    """
    import os
    from utils.ch_utils import transfer_table

    full_path = os.path.join(SQL_PATH, sql_file)
    with open(full_path, "r", encoding="utf-8") as f:
        query = f.read()

    transfer_table(pg_query=query, ch_table=f"data_mart.{table_name}", date_field=date_field)


# Определение DAG
with DAG(
    dag_id="dds_to_clickhouse",
    description="Загрузка витрин из DDS (Postgres) в ClickHouse",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["dds", "clickhouse", "data_mart"],
) as dag:

    load_daily_summary = PythonOperator(
        task_id="load_daily_summary",
        python_callable=run_transfer,
        op_args=["ch_insert_daily_business_summary.sql", "daily_business_summary"],
    )

    load_product_perf = PythonOperator(
        task_id="load_product_performance",
        python_callable=run_transfer,
        op_args=["ch_insert_product_performance.sql", "product_performance"],
    )

    load_category_perf = PythonOperator(
        task_id="load_category_performance",
        python_callable=run_transfer,
        op_args=["ch_insert_category_performance.sql", "category_performance"],
    )

    load_user_engagement = PythonOperator(
        task_id="load_user_engagement",
        python_callable=run_transfer,
        op_args=["ch_insert_user_engagement.sql", "user_engagement", "last_session_date"],
    )

    load_active_users = PythonOperator(
        task_id="load_active_users_7d",
        python_callable=run_transfer,
        op_args=["ch_insert_active_users_7d.sql", "active_users_7d"],
    )

    load_sessions = PythonOperator(
        task_id="load_session_analytics",
        python_callable=run_transfer,
        op_args=["ch_insert_session_analytics.sql", "session_analytics", "start_time"],
    )

    load_campaigns = PythonOperator(
        task_id="load_campaign_performance",
        python_callable=run_transfer,
        op_args=["ch_insert_campaign_performance.sql", "campaign_performance"],
    )

    load_geo = PythonOperator(
        task_id="load_geo_analysis",
        python_callable=run_transfer,
        op_args=["ch_insert_geo_analysis.sql", "geo_analysis"],
    )

    load_devices = PythonOperator(
        task_id="load_device_analysis",
        python_callable=run_transfer,
        op_args=["ch_insert_device_analysis.sql", "device_analysis"],
    )

    (
        load_daily_summary
        >> load_product_perf
        >> load_category_perf
        >> load_user_engagement
        >> load_active_users
        >> load_sessions
        >> load_campaigns
        >> load_geo
        >> load_devices
    )
