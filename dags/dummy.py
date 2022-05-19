from airflow.utils.dates import days_ago
from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
import sys

import json
from datetime import datetime
# from scrapeData import scrapeAllStations, scrape, scrapeAllData

with DAG('dummy',description='Scrape data every hour',tags=['pm2.5_dashboard'],schedule_interval='@hourly',start_date=datetime(2022, 5, 18),catchup = True) as dag:
    # scrapeDag = PythonOperator(task_id='scrapeAllStations', python_callable=scrapeAllStations,op_args=["{{ dag_run.logical_date | ts }}"])
    # scrapeDag
    dummy = DummyOperator(task_id='dummy')
    dummy

print("-------------------------------------------")
print(sys.version)