import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email

# Add the correct path inside the container
sys.path.append('/opt/airflow/producers')

from livescore_producer import fetch_and_produce as run_livescore_once

# Define callable for Airflow task
def run_live_score():
    run_livescore_once()

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

# Default args for DAG
default_args = {
    'owner': 'ahmed elsayad',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2025, 8, 8),
    'on_failure_callback': send_failure_email ,

}

# Define the DAG
with DAG(
    dag_id="live_score_dag",
    default_args=default_args,
    description='A DAG to orchestrate live score producer',
    catchup=False,
    schedule=timedelta(minutes=2),
    tags=['soccer.live_score'],
) as dag:

    task_run_live_score = PythonOperator(
        task_id='run_live_score',
        python_callable=run_live_score,
    )

    task_run_live_score
