import os
import datetime

from airflow import configuration
from airflow.models import DAG, Variable
from airflow.hooks.base_hook import BaseHook
from airflow.utils.task_group import TaskGroup
from plugins.operators.custom_kubernetes_operator import CustomKubernetesPodOperator


args = {
    'owner': 'airflow',
    'start_date': datetime.datetime(2017, 9, 12),
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=60)
}


dag = DAG(
        dag_id='example_dag_k8s', 
        default_args=args,
        schedule_interval='0 0 * * 0',
        user_defined_macros={'get_connection': BaseHook.get_connection},
        catchup=False,
        tags=['k8s', 'custom_k8s_pod_operator']
    )

with dag:

    hello_world = CustomKubernetesPodOperator(
                    dag=dag,
                    image="hello-world:latest",
                    name='hello-world'
                )

    hello_world