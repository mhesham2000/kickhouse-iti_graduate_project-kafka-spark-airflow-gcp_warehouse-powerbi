import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum
from airflow.utils.email import send_email

sys.path.append('/opt/airflow/producers')

from player_producer import run_once 

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
    dag_id="player_proucer_daily_dag",
    default_args=default_args,
    description='DAG to run player producer once daily at midnight Egypt time',
    catchup=False,
    schedule='0 0 * * *',  # every day at midnight
    tags=['soccer.player']
) as dag:

    task_run_player = PythonOperator(
        task_id='run_player_producer_once',
        python_callable=run_once,
    )

    task_run_player
