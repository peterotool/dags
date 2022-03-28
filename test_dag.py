from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG("test_task", start_date=datetime(2021, 1, 1),
         schedule_interval="@daily", catchup=False) as dag:

    bash_task = BashOperator(
        task_id="bash_task1",
        bash_command='gcloud auth activate-service-account --key-file=datalake-data-manager.json --project=emplipigas-datalake',
    )
    bash_task = BashOperator(
        task_id="bash_task2",
        bash_command='bq ls dl_clean_cl_sitrack',
    )
