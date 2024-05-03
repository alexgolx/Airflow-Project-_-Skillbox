import datetime as dt
import os
import sys

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import logging
import os
from datetime import datetime

import dill
import pandas as pd
from sklearn.compose import ColumnTransformer, make_column_selector
from sklearn.ensemble import RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import cross_val_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import FunctionTransformer
from sklearn.preprocessing import OneHotEncoder
from sklearn.preprocessing import StandardScaler
from sklearn.svm import SVC
from glob import glob
import json


path = os.path.expanduser('~/airflow_hw')
# Добавим путь к коду проекта в переменную окружения, чтобы он был доступен python-процессу
os.environ['PROJECT_PATH'] = path
# Добавим путь к коду проекта в $PATH, чтобы импортировать функции
sys.path.insert(0, path)

#path = os.path.expanduser(os.environ.get('PROJECT_PATH', '~/airflow_hw'))
# <YOUR_IMPORTS:
from modules.pipeline import pipeline
from modules.predict import predict


args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2024, 4, 30),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
    'depends_on_past': False,
}

with DAG(
        dag_id='car_price_prediction',
        schedule_interval="00 15 * * *",
        default_args=args,
) as dag:
    hello_message = BashOperator(
        task_id='hello_message',
        bash_command='echo "We are about to start!"',
        dag=dag,
    )

    pipeline_task = PythonOperator(
        task_id='pipeline_tag',
        python_callable=pipeline,
        dag = dag,
    )

    predict_task = PythonOperator(
        task_id = 'prediction_tag',
        python_callable = predict,
        dag = dag,
    )

    goodbye_message = BashOperator(
        task_id='goodbye_message',
        bash_command='echo "Thanks for flying our Airflow Airlines!"',
        dag=dag
    )

    hello_message >> pipeline_task >> predict_task >> goodbye_message


