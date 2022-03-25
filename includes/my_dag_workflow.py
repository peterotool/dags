from airflow.decorators import task
from airflow.operators.python import get_current_context


@task(multiple_outputs=True)
def download():
    return {'path': '/usr/local/airflow', 'filename': 'data.csv'}


@task
def clean(path):
    print('path :', path)


@task(task_id="spark")  # override task_id name
def process():
    context = get_current_context()
    ti = context['ti']
    ts = context['ts']
    print(f"task id_ {ti}")
    ti.xcom_push(key="result", value={'timestamp': ts})


@task
def report(fileinfo, processed_files):
    print(f"fileinfo: {fileinfo}")
    print(f"task processed_files: {processed_files}")
