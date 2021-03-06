from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import get_current_context

from airflow.models.xcom_arg import XComArg

from datetime import datetime


@task(multiple_outputs=True)
def download():
    return {'path': '/usr/local/airflow', 'filename': 'data.csv'}


@task
def clean(path):
    print('path :', path)


@task(task_id="spark")  # override task_id name
def process():
    #ti.xcom_push(key='processedfile', value={'timestamp': ts})
    context = get_current_context()
    ti = context['ti']
    ts = context['ts']
    print(f"task id_ {ti}")
    ti.xcom_push(key="result", value={'timestamp': ts})


@task
def report(fileinfo, processed_files):
    print(f"fileinfo: {fileinfo}")
    print(f"task processed_files: {processed_files}")


with DAG("dag04", start_date=datetime(2021, 1, 1),
         schedule_interval="@daily", catchup=False) as dag:

    fileinfo = download()
    cleaning = clean(fileinfo['path'])

    processed_files = []
    for process_task in range(1, 4):
        # processed_files.append(cleaning >> process()['processed_files'])
        processed_files.append(cleaning >> process()['result'])

    report(fileinfo, processed_files)
