from airflow import DAG
from includes.my_dag_workflow import download, clean, process, report
from airflow.models.xcom_arg import XComArg
from datetime import datetime


with DAG("dag05", start_date=datetime(2021, 1, 1),
         schedule_interval="@daily", catchup=False) as dag:

    fileinfo = download()
    cleaning = clean(fileinfo['path'])

    processed_files = []
    for process_task in range(1, 4):
        # processed_files.append(cleaning >> process()['processed_files'])
        processed_files.append(cleaning >> process()['result'])

    report(fileinfo, processed_files)
