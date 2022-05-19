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
,start_date= datetime(2022, 5, 13,17),catchup = True,max_active_runs=1) as dag:

    scrapeDag = PythonOperator(task_id='scrapeAllStations', python_callable=scrapeAllStations,op_args=["{{ dag_run.logical_date | ts }}"])
    delayDummy = BashOperator(task_id="dummy_delay_1",bash_command="sleep 30s")
    sendCurrentPM = PythonOperator(task_id='sendCurrentPM', python_callable=sendToBI,op_args=['history'])
    success = DummyOperator(task_id="success")
    scrapeDag >> delayDummy  >> sendCurrentPM >> success
    print("--------------- Success scrape ----------------")
    print(scrapeDag.output)
    print("-----------------------------------------------")
