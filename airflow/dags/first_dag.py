from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator

default_args = {
    'owner':'teja003',
    'retries':5,
    'retry_delay':timedelta(minutes=1)
}

with DAG(
    dag_id = "first_dag_v1",
    default_args=default_args,
    description = "This is my first Dag",
    start_date = datetime(2023,7,19),
    schedule_interval='@daily',
    catchup=False,
    tags=["basic"],
) as dag:
    task1 = BashOperator(
        task_id='first_task',
        bash_command = "echo hello"
    )
    task2 = BashOperator(
        task_id='second_task',
        bash_command = "echo task 2"
    )
    task1 >> task2