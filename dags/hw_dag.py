import datetime as dt
import os
import sys
import pandas as pd

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from modules.pipeline import pipeline
from modules.predict import predict

path = os.path.expanduser('~/airflow_hw')
# Добавим путь к коду проекта в переменную окружения, чтобы он был доступен python-процессу
os.environ['PROJECT_PATH'] = path
# Добавим путь к коду проекта в $PATH, чтобы импортировать функции


args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2024, 3, 31),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
    'depends_on_past': False,
}

with DAG(
        dag_id='car_price_prediction',
        schedule_interval="00 01 * * *",
        default_args=args
) as dag:
    pipeline = PythonOperator(
        task_id='pipeline',
        python_callable=pipeline,
        dag=dag
    )
    predict = PythonOperator(
        task_id='predict',
        python_callable=predict,
        dag=dag
    )

    pipeline >> predict
