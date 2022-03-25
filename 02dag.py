from airflow.utils.dates import days_ago
from airflow.models import DAG
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from airflow.models.xcom_arg import XComArg
# Class that represents a XCom push from a previous operator. Defaults to "return_value" as only key.

from datetime import datetime


def _download():
    return {'path': '/usr/local/airflow', 'filename': 'data.csv'}


def _clean(path, filename):
    print('path :', path)
    print('filename:', filename)


with DAG("my_dag_xcom_arg", start_date=datetime(2021, 1, 1),
         schedule_interval="@daily", catchup=False) as dag:
    # minuto 26
    download_output = PythonOperator(
        task_id="download",
        python_callable=_download
    ).output  # is not an Operator but and XComArg object

    clean = PythonOperator(
        task_id="clean",
        python_callable=_clean,
        op_kwargs=download_output
    )


# with DAG(
#     "xcom_args_are_awesome",
#     default_args={'owner': 'airflow'},
#     start_date=days_ago(2),
#     schedule_interval=None,
# ) as dag:
#     numbers = PythonOperator(
#         task_id="numbers",
#         python_callable=lambda: list(range(10))
#     )
#     show = PythonOperator(
#         task_id="show",
#         python_callable=lambda x: print(x),
#         op_args=(numbers.output,)
#     )
