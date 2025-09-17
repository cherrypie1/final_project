from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from utils.telegram_alerts import task_success_alert, task_fail_alert
from utils.ch_utils import transfer_table

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 9, 1),
    "retries": 1,
    "on_success_callback": task_success_alert,
    "on_failure_callback": task_fail_alert,
}

with DAG(
    dag_id="dds_to_clickhouse",
    description="Загрузка витрин из DDS в ClickHouse",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    template_searchpath=["/opt/airflow/sql"],
) as dag:

    # Ежедневная сводка

    load_daily_summary = PythonOperator(
        task_id="load_daily_summary",
        python_callable=transfer_table,
        op_args=["ch_insert_daily_business_summary.sql", "daily_business_summary"],
    )

    # Продажи по товарам

    load_product_perf = PythonOperator(
        task_id="load_product_performance",
        python_callable=transfer_table,
        op_args=["ch_insert_product_performance.sql", "product_performance"],
    )

    # Продажи по категориям

    load_category_perf = PythonOperator(
        task_id="load_category_performance",
        python_callable=transfer_table,
        op_args=["ch_insert_category_performance.sql", "category_performance"],
    )

    # Вовлеченность пользователей

    load_user_engagement = PythonOperator(
        task_id="load_user_engagement",
        python_callable=transfer_table,
        op_args=["ch_insert_user_engagement.sql", "user_engagement"],
    )

    # Активные пользователи 7д

    load_active_users = PythonOperator(
        task_id="load_active_users_7d",
        python_callable=transfer_table,
        op_args=["ch_insert_active_users_7d.sql", "active_users_7d"],
    )

    # Сессии

    load_sessions = PythonOperator(
        task_id="load_session_analytics",
        python_callable=transfer_table,
        op_args=["ch_insert_session_analytics.sql", "session_analytics"],
    )

    # Кампании

    load_campaigns = PythonOperator(
        task_id="load_campaign_performance",
        python_callable=transfer_table,
        op_args=["ch_insert_campaign_performance.sql", "campaign_performance"],
    )

    # Гео

    load_geo = PythonOperator(
        task_id="load_geo_analysis",
        python_callable=transfer_table,
        op_args=["ch_insert_geo_analysis.sql", "geo_analysis"],
    )

    # Устройства

    load_devices = PythonOperator(
        task_id="load_device_analysis",
        python_callable=transfer_table,
        op_args=["ch_insert_device_analysis.sql", "device_analysis"],
    )

    (
        load_daily_summary >> load_product_perf >> load_category_perf
        >> load_user_engagement >> load_active_users >> load_sessions
        >> load_campaigns >> load_geo >> load_devices
    )
