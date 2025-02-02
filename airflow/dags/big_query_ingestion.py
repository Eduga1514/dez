# Import dependencies
import os
from datetime import datetime

import pandas as pd
import gzip

import logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

from airflow.decorators import dag, task, task_group
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow import AirflowException

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

def generate_urls(years:list):

    if not isinstance(years, (int, list)):
        raise ValueError('Value entered for year is not an Integer or List, please input an Integer or List.')


    url_prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow'
    urls = {}

    for year in years:
        urls_by_year = []

        for i in range(12):
            month = i + 1
            if month==1 and year==2019:
                continue
            
            urls_by_year.append(f'{url_prefix}/yellow_tripdata_{year}-{month:02d}.csv.gz')

            if year==2021 and month==7:
                break
        
        urls[year] = urls_by_year

    return urls

dag_default_args = {
    'owner': 'Eduga'
}
@dag(
    dag_id='big_query_pipeline',
    description='Manage taxi data ingestion to a Big Query dataset, with staging done in GCS',
    start_date=datetime(2024, 1, 4),
    schedule_interval='@daily',
    tags=['bigquery', 'gcs', 'taxi', 'dez'],
    default_view='graph',
    catchup=False,
    concurrency=3,
    max_active_runs=1,
    default_args=dag_default_args,
)
def big_query_pipeline():

    urls_dict = generate_urls([2019, 2020, 2021])

    for year in urls_dict.keys():
        @task_group(group_id=f'ingest_yellow_taxi_{year}')
        def ingest_yellow_taxi(urls):
            for url in urls:
                file_name = url.split('/')[-1]
                file_name_without_suffix = file_name.split('.')[0]
                file_path = AIRFLOW_HOME + '/' + file_name
                csv_path = file_name.replace('.gz', '')

                @task_group(group_id=f'data_fetch_and_ingestion_{file_name_without_suffix}')
                def create_data_fetch_and_ingestion_tasks(bucket_name, file):

                    @task.bash(task_id=f'fetch_{file_name_without_suffix}')
                    def fetch(url, file_path) -> str:

                        return f'curl -sSL {url} > {file_path}'
                    
                    @task(task_id=f'gcs_staging_{file_name_without_suffix}')
                    def gcs_staging(bucket_name, path, csv_path):
                        
                        try:
                            with gzip.open(path, 'rb') as f_in, open(csv_path, 'wb') as f_out:
                                f_out.write(f_in.read())

                            hook = GCSHook()
                            hook.upload(
                                bucket_name=bucket_name,
                                object_name=f'yellow_taxi_data/{os.path.basename(csv_path)}',
                                filename=csv_path
                            )

                            os.remove(path)
                            os.remove(csv_path)
                        except AirflowException as e:
                            logging.error(".csv file was not uploaded to GCS. Error: {e}")

                    ingest_to_bq_instance = GCSToBigQueryOperator(
                        task_id=f'ingest_{file_name_without_suffix}_to_bq',
                        bucket=bucket_name,
                        source_objects=f'yellow_taxi_data/{csv_path}',
                        destination_project_dataset_table=f"dez-bootcamp.yellow_taxi.yellow_trips",
                        write_disposition="WRITE_APPEND",
                        time_partitioning={
                            'type': 'DAY',
                            'field': 'tpep_pickup_datetime'
                        }
                    )
                        
                    fetch_instance = fetch(url, file_path)
                    gcs_instance   = gcs_staging(bucket_name, file_path, csv_path)

                    fetch_instance >> gcs_instance >> ingest_to_bq_instance

                create_data_fetch_and_ingestion_tasks('dez-bootcamp-terraform-bucket', url)
        
        ingest_yellow_taxi(urls_dict[year])

big_query_pipeline()