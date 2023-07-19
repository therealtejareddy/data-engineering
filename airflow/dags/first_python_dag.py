from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner':'teja003',
    'retries':5,
    'retry_delay':timedelta(minutes=1)
}

def simple_fun():
    print("Hello")

def param_fun(name,age):
    print(f"{name} {age}")
    return "teja"

def third_op(ti):
    name = ti.xcom_pull(task_ids="second_task")
    ti.xcom_push(key="name",value="teja")
    ti.xcom_push(key="age",value=20)
    print(name)

def pull_xcom(ti):
    print(ti.xcom_pull(task_ids="third_task",key="age"))

with DAG(
    dag_id = "first_python_dag_v1",
    default_args=default_args,
    description = "This is my first Python Dag",
    start_date = datetime(2023,7,19),
    schedule_interval='@daily',
    # hourly, daily, weekly, monthly, yearly
) as dag:
    task1 = PythonOperator(
        task_id='first_task',
        python_callable=simple_fun
    )
    task2 = PythonOperator(
        task_id='second_task',
        python_callable=param_fun,
        op_kwargs={'name':"Teja",'age':21}
    )
    task3 = PythonOperator(
        task_id="third_task",
        python_callable=third_op
    )
    task4 = PythonOperator(
        task_id="end_task",
        python_callable=pull_xcom
    )
    task1 >> task2 >> task3 >> task4