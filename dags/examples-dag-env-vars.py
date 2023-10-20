from airflow import DAG
from datetime import datetime
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator
from plugins.env import get_s3_env_vars, get_database_env_vars
from plugins.macros import build_postgres_conn_string

def print_s3_env_vars(s3_env_vars):
    """Imprime les variables d'environnement."""
    for key, value in s3_env_vars.items():
        print(f"s3 {key}: {value}")
        
def print_db_env_vars(db_env_vars):
    """Imprime les variables d'environnement."""
    for key, value in db_env_vars.items():
        print(f"dv {key}: {value}")


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 20),
}

with DAG('test_env_vars_dag',
         default_args=default_args,
         schedule_interval='@daily',
         user_defined_macros={
             'get_connection': BaseHook.get_connection, 
             'build_conn_string': build_postgres_conn_string
            },
         catchup=False) as dag:
    
    # Obtenez les variables d'environnement en dehors des tâches.
    s3_env_vars = get_s3_env_vars('s3_conn')
    db_env_vars = get_database_env_vars('pg_conn')

    print_s3_vars_task = PythonOperator(
        task_id='print_s3_vars',
        python_callable=print_s3_env_vars,
        op_kwargs={'s3_env_vars': s3_env_vars}
    )
    
    print_db_vars_task = PythonOperator(
        task_id='print_db_vars',
        python_callable=print_db_env_vars,
        op_kwargs={'db_env_vars': db_env_vars}
    )

    print_s3_vars_task >> print_db_vars_task
