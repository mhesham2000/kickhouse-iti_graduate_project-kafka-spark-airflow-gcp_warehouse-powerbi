from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email
from datetime import datetime, timedelta

# Task that always fails
def fail_task():
    raise ValueError("This task is meant to fail")

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

# Default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 8, 14),
    'retries': 1,
    'retry_delay': timedelta(seconds=3),
    'on_failure_callback': send_failure_email ,
    # 'email_on_failure': True ,
    # 'email_on_retry': True,
}

# DAG definition
with DAG(
    'test_failure_email_dag',
    default_args=default_args,
    schedule=None,
    catchup=False
) as dag:

    failing_task = PythonOperator(
        task_id='fail_task',
        python_callable=fail_task
    )
