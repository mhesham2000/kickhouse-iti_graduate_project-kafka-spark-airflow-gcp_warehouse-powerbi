import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum
from airflow.utils.email import send_email

sys.path.append('/opt/airflow/producers')

from event_lookup_producer import main_loop as lookup_main_loop
from livescore_producer import fetch_and_produce as score_main_loop 

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
    'start_date': datetime(2025, 8, 10),
    'on_failure_callback': send_failure_email ,

}

with DAG(
    dag_id="event_lookup_dag",
    default_args=default_args,
    description='A DAG to orchestrate the event lookup and live score producers',
    catchup=False,
    schedule=timedelta(minutes=2),
    tags=['soccer.live.event.lookup', 'soccer.live_score'],
) as dag:

    task_run_lookup = PythonOperator(
        task_id='run_live_lookup',
        python_callable=lookup_main_loop,
    )

    task_run_live_score = PythonOperator(
        task_id='run_live_score',
        python_callable=score_main_loop,
    )

task_run_live_score >> task_run_lookup
