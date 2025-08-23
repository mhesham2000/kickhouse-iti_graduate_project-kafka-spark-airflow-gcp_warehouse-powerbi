import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum
from airflow.utils.email import send_email

sys.path.append('/opt/airflow/producers')

from schedule_producer import fetch_and_produce 

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
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2025, 8, 8, tzinfo=egypt_tz),
    'on_failure_callback': send_failure_email ,

}

with DAG(
    dag_id="schedual_proucer_daily_dag",
    default_args=default_args,
    description='DAG to run schedual producer once daily at midnight Egypt time',
    catchup=False,
    schedule='0 0 * * *',  # every day at midnight
    tags=['soccer.schedule']
) as dag:

    task_run_schedule = PythonOperator(
        task_id='run_schedule_producer_once',
        python_callable=fetch_and_produce,
    )

    task_run_schedule
