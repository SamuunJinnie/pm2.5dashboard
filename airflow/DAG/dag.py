from airflow.scraping.scrapeData import scrapeAllStation
from airflow.utils.dates import days_ago
from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

with DAG(
    'scrape',
    description='Scrape data every hour',
    tags=['pm2.5_dashboard'],
    schedule_interval='@hourly',
    catchup = False
) as dag:
    scrape = PythonOperator(task_id='scrapeAllStations', python_callable=scrapeAllStations)


    scrape
