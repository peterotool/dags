from airflow.utils.dates import days_ago
from airflow.models import DAG
from airflow import DAG
from airflow.operators.python import get_current_context
from airflow.decorators import task


from airflow.models.xcom_arg import XComArg

from datetime import datetime


@task
def download():
    return {'path': '/usr/local/airflow', 'filename': 'data.csv'}


@task
def clean(fileinfo):
    print('path :', fileinfo['path'])
    print('filename:', fileinfo['filename'])


@task
def process():
    #ti.xcom_push(key='processedfile', value={'timestamp': ts})
    context = get_current_context()
    ti = context['ti']
    ts = context['ts']
    print('process the data')


with DAG("dag03", start_date=datetime(2021, 1, 1),
         schedule_interval="@daily", catchup=False) as dag:

    cleaning = clean(download())

    for process_task in range(1, 4):
        cleaning >> process()
