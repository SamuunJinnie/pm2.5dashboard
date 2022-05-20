import pytz
from airflow.utils.dates import days_ago
from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator

import json
from datetime import datetime, timedelta, timezone
from scrapeData import scrapeAllStations, scrape, scrapeAllData

#start date use utc+0

with DAG('daily_dag',default_args={'retries': 5,'retry_delay': timedelta(seconds=30)}, description='Train and predict daily',tags=['pm2.5_dashboard'],schedule_interval='@daily'
,start_date= datetime(2022, 5, 13,17),catchup = True,max_active_runs=1) as dag:

    scrapeDag = PythonOperator(task_id='scrapeAllStations', python_callable=scrapeAllStations,op_args=["{{ dag_run.logical_date | ts }}"])
    dummy = DummyOperator(task_id='finish')
    scrapeDag >> dummy
    print("--------------- Success scrape ----------------")
    print(scrapeDag.output)
    print("-----------------------------------------------")