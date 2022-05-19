import pytz
from airflow.utils.dates import days_ago
from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
import sys

import json
from datetime import datetime, timezone
# from scrapeData import scrapeAllStations, scrape, scrapeAllData

startDate_utc0 = datetime(2022, 5, 18,tzinfo=timezone.utc)
startDate_utc7 = startDate_utc0.replace(tzinfo=timezone.utc).astimezone(tz=pytz.timezone('Asia/Bangkok'))

with DAG('dummy',description='Scrape data every hour',tags=['pm2.5_dashboard']
,schedule_interval='@hourly',start_date=startDate_utc7,catchup = True) as dag:
    # scrapeDag = PythonOperator(task_id='scrapeAllStations', python_callable=scrapeAllStations,op_args=["{{ dag_run.logical_date | ts }}"])
    # scrapeDag
    dummy = DummyOperator(task_id='dummy')
    dummy

print("-------------------------------------------")
print(sys.version)
