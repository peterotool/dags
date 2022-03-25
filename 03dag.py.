from airflow.operators.python import PythonOperator
from airflow.models import DAG
from airflow.utils.dates import days_ago
with DAG(
    "xcom_args_are_awesome",
    default_args={'owner': 'airflow'},
    start_date=days_ago(2),
    schedule_interval=None,
) as dag:
    numbers = PythonOperator(
        task_id="numbers",
        python_callable=lambda: list(range(10))
    )
    show = PythonOperator(
        task_id="show",
        python_callable=lambda x: print(x),
        op_args=(numbers.output,)
    )
