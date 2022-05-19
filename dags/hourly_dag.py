from airflow.utils.dates import days_ago
from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator

from datetime import datetime, timedelta, timezone
from dags.service.senderBIAPI import sendToBI
from service.scrapeData import scrapeAllStations, scrape, scrapeAllData
from service.senderBIAPI import sendToBI

#start date use utc+0

with DAG('hourly_dag',default_args={'retries': 5,'retry_delay': timedelta(seconds=30)}, description='Scrape & Save data every hour',tags=['pm2.5_dashboard'],schedule_interval='@hourly'
,start_date= datetime(2022, 5, 13,17),catchup = True,max_active_runs=5) as dag:

    scrapeDag = PythonOperator(task_id='scrapeAllStations', python_callable=scrapeAllStations,op_args=["{{ dag_run.logical_date | ts }}"])
    dummyScrape = DummyOperator(task_id='Success_Scrape')
    dummySend = DummyOperator(task_id='Success_Send_Current')
    sendCurrentPM = PythonOperator(task_id='sendCurrentPM', python_callable=sendToBI('history'))
    scrapeDag >> dummyScrape >> sendCurrentPM >> dummySend
    print("--------------- Success scrape ----------------")
    print(scrapeDag.output)
    print("-----------------------------------------------")