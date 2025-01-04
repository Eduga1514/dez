from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    owner: 'Eduga',
    'on_failure_callback': on_failure_callback,
}

@task.bash(task_id='wget')
def wget() -> str:
    return 'echo "hello world"'

@task.bash(task_id='wget')
def ingest() -> str:
    return 'echo "hello world again"'


@dag(
    dag_id=local_workflow,
    description='Manage taxi data ingestion to local postgres instance',
    schedule_interval='@daily'
    tags=['local', 'postgres', 'taxi', 'dez'],
    default_view='graph',
)
def local_workflow():
    wget_instance = wget()
    ingest_instance = ingest()

    wget_instance >> ingest_instance