import datetime

from airflow.models import DAG
from airflow.hooks.base import BaseHook
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator
from plugins.operators.custom_kubernetes_operator import CustomKubernetesPodOperator

args = {
    'owner': 'airflow',
    'start_date': datetime.datetime(2023, 9, 12),
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=60)
}


dag = DAG(
        dag_id='inf7225_fountains_optimizer_dag', 
        default_args=args,
        concurrency=2,
        max_active_runs=1,
        schedule_interval='@daily',
        catchup=False,
        tags=['uqam', 'inf7225', 'fountains_optimizer'],
        user_defined_macros = {"get_connection": BaseHook.get_connection}
    )

s3_env_vars = {
    "S3_ENDPOINT": "{{ get_connection('s3_conn').host }}",
    "S3_ACCESS_KEY": "{{ get_connection('s3_conn').login }}",
    "S3_SECRET_KEY": "{{ get_connection('s3_conn').password }}",
    "S3_BUCKET": "{{ get_connection('s3_conn').schema }}",
    }

urban_infrastructures = {
    "outdoor-drinking-fountains-data": {
        "prefix": "outdoor-drinking-fountains",
        "url": "https://donnees.montreal.ca/dataset/3ff400f3-63cd-446d-8405-842383377fb8/resource/26659739-540d-4fe2-8107-5f35ab7e807c/download/fontaine_eau_potable_v2018.csv"
    },
    "urban-green-streets-data": {
        "prefix": "urban-green-streets",
        "url": "https://data.montreal.ca/dataset/ab3ce7bb-09a7-49d7-8f76-461ed4c39937/resource/15883136-0180-4061-9860-d7ce3d46c73c/download/ruelles-vertes.geojson"
    },
    "cycling-network-data": {
        "prefix": "cycling-network",
        "url": "https://donnees.montreal.ca/dataset/5ea29f40-1b5b-4f34-85b3-7c67088ff536/resource/0dc6612a-be66-406b-b2d9-59c9e1c65ebf/download/reseau_cyclable.geojson"
    },
    "express-bike-network-data": {
        "prefix": "express-bike-network",
        "url": "https://donnees.montreal.ca/dataset/8a4bf03c-dff6-4add-b58b-c38954b0ed0d/resource/8ad67029-cf2e-49ae-a4b6-20d31611ab6e/download/reseau-express-velo.geojson"
    },
    "park-furnishings-data": {
        "prefix": "park-furnishings",
        "url": "https://donnees.montreal.ca/dataset/fb04fa09-fda1-44df-b575-1d14b2508372/resource/65766e31-f186-4ac9-9595-bfcf47ae9158/download/mobilierurbaingp.geojson"
    },
    "large-parks-and-public-spaces-data": {
        "prefix": "large-parks-and-public-spaces",
        "url": "https://donnees.montreal.ca/dataset/2e9e4d2f-173a-4c3d-a5e3-565d79baa27d/resource/35796624-15df-4503-a569-797665f8768e/download/espace_vert.json"
    },
    "exterior-sports-cultural-installations-data": {
        "prefix": "exterior-sports-cultural-installations",
        "url": "https://donnees.montreal.ca/dataset/60850740-dd83-47ee-9a19-13d674e90314/resource/2dac229f-6089-4cb7-ab0b-eadc6a147d5d/download/terrain_sport_ext.json"
    }
}

with dag:
    
    begin = DummyOperator(task_id="begin")
    end = DummyOperator(task_id="end")

    
    with TaskGroup(group_id="retrieve-raw-urban-data-pod") as retrieve_raw_urban_data_pod:
        for key, value in urban_infrastructures.items():
            task = CustomKubernetesPodOperator(
                dag=dag,
                name=f"retrieve-{key}-pod",
                image=f"uqam/{value['prefix']}-img:1.0.0",
                env_vars={**s3_env_vars},
                arguments=[
                    "--command", "fetch_data",
                    "--output_file_key", f"raw-{value['prefix']}.pickle",
                    "--url", f"{value['url']}"
                    ]
                )


    with TaskGroup(group_id="transform-urban-data-pod") as transform_urban_data_pod:
        for key, value in urban_infrastructures.items():
            task = CustomKubernetesPodOperator(
                dag=dag,
                name=f"transform-{key}-pod",
                image=f"uqam/{value['prefix']}-img:1.0.0",
                env_vars={**s3_env_vars},
                arguments=[
                    "--command", "transform_data",
                    "--input_file_key", f"raw-{value['prefix']}.pickle",
                    "--output_file_key", f"{value['prefix']}.pickle",
                    "--url", f"{value['url']}"
                    ]
                )


    begin >> retrieve_raw_urban_data_pod >> transform_urban_data_pod >> end
