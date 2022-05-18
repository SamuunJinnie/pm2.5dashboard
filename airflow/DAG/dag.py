from airflow.utils.dates import days_ago
from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

with DAG(
    'scrape',
    default_args=default_args,
    description='Scrape data every hour',
    tags=['pm2.5_dashboard'],
    schedule_interval='@hourly',
    catchup = False
) as dag:
    scrape = PythonOperator(task_id='inform_status', python_callable=show_status, op_args=[ping.output, ping2.output])
