import requests

TELEGRAM_BOT_TOKEN = "8222480311:AAHSHMyKRkPbfQ94yJqqjCo0LhThLInnYIQ"
TELEGRAM_CHAT_ID = "379641390"


def send_telegram_message(message: str):
    # функция отправки сообщений
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            data={"chat_id": TELEGRAM_CHAT_ID, "text": message}
        )
    except Exception as e:
        print(f"[ERROR] Telegram send failed: {e}")


def task_fail_alert(context):
    # Уведомление об ошибке
    dag_id = context.get('dag').dag_id
    task_id = context.get('task_instance').task_id
    execution_date = context.get('execution_date')
    log_url = context.get('task_instance').log_url

    message = (
        f"❌ Airflow Task Failed\n"
        f"DAG: {dag_id}\n"
        f"Task: {task_id}\n"
        f"Execution time: {execution_date}\n"
        f"Logs: {log_url}"
    )
    send_telegram_message(message)


def task_success_alert(context):
    # Уведомление об успешном завершении
    dag_id = context.get('dag').dag_id
    task_id = context.get('task_instance').task_id
    execution_date = context.get('execution_date')
    log_url = context.get('task_instance').log_url

    message = (
        f"✅ Airflow Task Success\n"
        f"DAG: {dag_id}\n"
        f"Task: {task_id}\n"
        f"Execution time: {execution_date}\n"
        f"Logs: {log_url}"
    )
    send_telegram_message(message)
