from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from airflow.models.xcom_arg import XComArg
# Class that represents a XCom push from a previous operator. Defaults to "return_value" as only key.

from datetime import datetime


def _download(ti):
    ti.xcom_push(key='fileinfo', value={'path': '/usr/local/airflow', 'filename': 'data.csv'})


def _clean(ti):
    fileinfo = ti.xcom_pull(key='fileinfo', task_ids=['download'])[0]

    print('fileinfo:', fileinfo)

    xcom_arg = XComArg(download)
    print('xcom_arg :', xcom_arg['fileinfo'])

    print(download.output['file'])
    print(download.output.get('file'))

    print(f"clean the data:")


# def _process(ts, ti):
#     print('process the data')
#     ti.xcom_push(key='processedfile', value={'timestamp': ts})


# def _report(ti):
#     info = ti.xcom_pull(key=None, task_ids=['download', 'processedfile'])[0]
#     print(f"Report: {info}")


with DAG("my_dag_xcom_arg", start_date=datetime(2021, 1, 1),
         schedule_interval="@daily", catchup=False) as dag:

    download = PythonOperator(
        task_id="download",
        python_callable=_download
    )

    clean = PythonOperator(
        task_id="clean",
        python_callable=_clean
    )

    # process = PythonOperator(
    #     task_id="process",
    #     python_callable=_process
    # )

    # report = PythonOperator(
    #     task_id="report",
    #     python_callable=_report
    # )

    download >> clean  # >> process >> report
