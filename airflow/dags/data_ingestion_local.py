import os
from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from ingestion_script import ingest

URL_PREFIX = 'https://d37ci6vzurychx.cloudfront.net/'
PATHS = [ 'misc/taxi_zone_lookup.csv', 'trip-data/green_tripdata_2019-10.parquet' ]

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
PG_USER = "root"
PG_PASSWORD = "root"
PG_HOST = "pg-database"
PG_DATABASE = "ny_taxi"
PG_PORT = "5432"


dag_default_args = {
    'owner': 'Eduga',
}

@dag(
    dag_id='local_workflow',
    description='Manage taxi data ingestion to local postgres instance',
    start_date=datetime(2024, 1, 4),
    schedule_interval='@daily',
    tags=['local', 'postgres', 'taxi', 'dez'],
    default_view='graph',
    catchup=False,
    default_args=dag_default_args,
)
def local_workflow():

    def create_download_and_ingestion_tasks(path):
        url = URL_PREFIX + path

        file_name = path.split('/')[-1]
        file_name_no_suffix = file_name.split('.')[0]

        file_path = AIRFLOW_HOME + '/' + file_name

        @task.bash(task_id=f'curl_{file_name_no_suffix}')
        def curl(url, path) -> str:
            return f'curl -sS {url} > {file_path}'

        @task(task_id=f'ingestion_{file_name_no_suffix}')
        def ingestion(table_name, file_name):

            ingest(
                user=PG_USER,
                password=PG_PASSWORD,
                host=PG_HOST,
                db=PG_DATABASE,
                port=PG_PORT,
                table_name=table_name,
                file_name=file_name
            )
            
        curl_instance = curl(url, path)
        ingest_instance = ingestion(file_name_no_suffix, file_path)

        curl_instance >> ingest_instance

    for p in PATHS:
        create_download_and_ingestion_tasks(p)

local_workflow()