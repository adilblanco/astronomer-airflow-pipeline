from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'hello_world',
    default_args=default_args,
    description='A simple Hello World DAG',
    schedule_interval='0 0 * * *',
    catchup=False,
    tags=['uqam', 'inf7225', 'hello_world']
)

with dag:

    t1 = BashOperator(
        task_id='print_hello',
        bash_command='echo "Hello World"',
        dag=dag,
    )

    t1
