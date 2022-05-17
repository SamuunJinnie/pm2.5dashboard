from airflow.utils.dates import days_ago
from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator

import json
from datetime import datetime
from scrapeData import scrapeAllStations

with DAG('scrape',description='Scrape data every hour',tags=['pm2.5_dashboard'],schedule_interval='@hourly',start_date=datetime(2022, 5, 17),catchup = True) as dag:
    scrapeDag = PythonOperator(task_id='scrapeAllStations', python_callable=scrapeAllStations,op_args=["{{ dag_run.logical_date | ts }}"])
    scrapeDag
    # dummyEiEi = PythonOperator(task_id='scrapeAllStations', python_callable=dummy,op_args=["{{ dag_run.logical_date | ts }}"])
    # dummyEiEi
