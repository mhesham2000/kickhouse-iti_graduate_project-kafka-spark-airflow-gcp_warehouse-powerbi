import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email

sys.path.append('/opt/airflow/producers')

from broadcast_producer import runonce as run_broadcast_once

def run_broadcast():
    run_broadcast_once()

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
    'email_on_failure': True,
    'email': ['shinetym@gmail.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2025, 8, 8),
    'on_failure_callback': send_failure_email ,

}

with DAG(
    dag_id="broadcast_dag",
    default_args=default_args,
    description='A DAG to orchestrate the broadcast producer',
    catchup=False,
    schedule=timedelta(minutes=2),
    tags=['soccer.broadcast'],
) as dag:

    task_run_broadcast = PythonOperator(
        task_id='run_broadcast',
        python_callable=run_broadcast,
    )

    task_run_broadcast
