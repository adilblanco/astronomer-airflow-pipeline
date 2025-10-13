import datetime
from airflow.models import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from plugins.operators.custom_kubernetes_operator import CustomKubernetesPodOperator

args = {
    'owner': 'airflow',
    'start_date': datetime.datetime(2025, 1, 1),
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=60)
}


dag = DAG(
        dag_id='mgl7161_dag', 
        default_args=args,
        concurrency=2,
        max_active_runs=1,
        schedule_interval='@daily',
        catchup=False,
        tags=['uqam', 'mgl7361', 'open-data', 'urban-infrastructures'],
        user_defined_macros = {"get_connection": BaseHook.get_connection}
    )

s3_env_vars = {
    "S3_ENDPOINT": "{{ get_connection('s3_conn').host }}",
    "S3_ACCESS_KEY": "{{ get_connection('s3_conn').login }}",
    "S3_SECRET_KEY": "{{ get_connection('s3_conn').password }}",
    "S3_BUCKET": "{{ get_connection('s3_conn').schema }}",
    }

with dag:
    
    begin = DummyOperator(task_id="begin")
    end = DummyOperator(task_id="end")

    ########################################################
    # https://donnees.montreal.ca/dataset/pistes-cyclables
    # pistes cyclables geojson
    ########################################################
    retrieve_cycling_network = CustomKubernetesPodOperator(
        dag=dag,
        name="retrieve-cycling-network",
        image="mgl7361/cycling-network-img:v1.0.0",
        env_vars={**s3_env_vars},
        arguments=[
            "--command", "fetch_data",
            "--output_file_key", "raw-cycling-network.pickle",
            "--url", "https://donnees.montreal.ca/dataset/5ea29f40-1b5b-4f34-85b3-7c67088ff536/resource/0dc6612a-be66-406b-b2d9-59c9e1c65ebf/download/reseau_cyclable.geojson"
            ]
        )

    transform_cycling_network = CustomKubernetesPodOperator(
                dag=dag,
                name="transform-cycling-network",
                image="mgl7361/cycling-network-img:v1.0.0",
                env_vars={**s3_env_vars},
                arguments=[
                    "--command", "transform_data",
                    "--input_file_key", "raw-cycling-network.pickle",
                    "--output_file_key", "cycling-network.pickle",
                    ]
                )
    
    ########################################################
    # https://donnees.montreal.ca/dataset/installations-recreatives-sportives-et-culturelles
    # Installations récréatives, sportives et culturelles extérieures 
    ########################################################
    retrieve_exterior_sports_installations = CustomKubernetesPodOperator(
        dag=dag,
        name="retrieve-exterior-sports-installations",
        image="mgl7361/exterior-sports-cultural-installations-img:v1.0.0",
        env_vars={**s3_env_vars},
        arguments=[
            "--command", "fetch_data",
            "--output_file_key", "raw-exterior-sports-installations.pickle",
            "--url", "https://donnees.montreal.ca/dataset/60850740-dd83-47ee-9a19-13d674e90314/resource/2dac229f-6089-4cb7-ab0b-eadc6a147d5d/download/terrain_sport_ext.json"
            ]
        )

    transform_exterior_sports_installations = CustomKubernetesPodOperator(
                dag=dag,
                name="transform-exterior-sports-installations",
                image="mgl7361/exterior-sports-cultural-installations-img:v1.0.0",
                env_vars={**s3_env_vars},
                arguments=[
                    "--command", "transform_data",
                    "--input_file_key", "raw-exterior-sports-installations.pickle",
                    "--output_file_key", "exterior-sports-installations.pickle",
                    ]
                )
    
    ########################################################
    # https://donnees.montreal.ca/dataset/grands-parcs-parcs-d-arrondissements-et-espaces-publics
    # Grands parcs, parcs d'arrondissements et espaces publics
    ########################################################
    retrieve_large_green_space = CustomKubernetesPodOperator(
        dag=dag,
        name="retrieve-large-green-space",
        image="mgl7361/large-parks-and-public-spaces-img:v1.0.0",
        env_vars={**s3_env_vars},
        arguments=[
            "--command", "fetch_data",
            "--output_file_key", "raw-large-green-space.pickle",
            "--url", "https://donnees.montreal.ca/dataset/2e9e4d2f-173a-4c3d-a5e3-565d79baa27d/resource/35796624-15df-4503-a569-797665f8768e/download/espace_vert.json"
            ]
        )

    transform_large_green_space = CustomKubernetesPodOperator(
                dag=dag,
                name="transform-large-green-space",
                image="mgl7361/large-parks-and-public-spaces-img:v1.0.0",
                env_vars={**s3_env_vars},
                arguments=[
                    "--command", "transform_data",
                    "--input_file_key", "raw-large-green-space.pickle",
                    "--output_file_key", "large-green-space.pickle",
                    ]
                )


    begin >> retrieve_cycling_network >> transform_cycling_network >> end
    begin >> retrieve_large_green_space >> transform_large_green_space >> end
    begin >> retrieve_exterior_sports_installations >> transform_exterior_sports_installations >> end
