from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum
from airflow.utils.email import send_email

# Failure callback that sends email
def send_failure_email(context):
    try:
        ti = context['ti']
        dag_run = context.get('dag_run')
        execution_time = dag_run.logical_date if dag_run else 'N/A'

        send_email(
            to=['shinetym@gmail.com'],
            subject=f"DAG {ti.dag_id} - Task {ti.task_id} FAILED",
            html_content=f"""
            Task: {ti.task_id} <br>
            DAG: {ti.dag_id} <br>
            Execution Time: {execution_time} <br>
            Exception: {context.get('exception')} <br>
            """
        )
        print("Failure email sent successfully")
    except Exception as e:
        print(f"Failed to send failure email: {e}")

egypt_tz = pendulum.timezone("Africa/Cairo")

default_args = {
    'owner': 'ahmed elsayad',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2025, 8, 8, tzinfo=egypt_tz),
    'on_failure_callback': send_failure_email ,

}

with DAG(
    dag_id="save_rejected_topics_as_parquet_daily",
    default_args=default_args,
    description='Consume Kafka invalid topics daily and save to Parquet',
    catchup=False,
    schedule='0 6 * * *',  # Every day at 06:00 am Cairo time
    tags=['kafka', 'parquet', 'soccer']
) as dag:

    consume_kafka_task = BashOperator(
        task_id='consume_kafka_and_save_parquet',
        bash_command="python /opt/airflow/scripts/consume_kafka.py"
    )

    consume_kafka_task
