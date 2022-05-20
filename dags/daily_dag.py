from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta, timezone
from service.senderBIAPI import sendToBI
from service.train import inputModel

#start date use utc+0

with DAG('daily_dag',default_args={'retries': 5,'retry_delay': timedelta(seconds=30)}, description='Train and predict daily',tags=['pm2.5_dashboard'],schedule_interval='@daily'
,start_date= datetime(2022, 5, 19,17),catchup = True,max_active_runs=1) as dag:

    feedModel = PythonOperator(task_id='feedModel', python_callable=inputModel)
    delayDummy = BashOperator(task_id="dummy_delay_1",bash_command="sleep 60s")
    sendPredictedPM = PythonOperator(task_id='sendPredictedPM', python_callable=sendToBI,op_args=['predicted'])
    success = DummyOperator(task_id="success")

    feedModel >> delayDummy  >> sendPredictedPM >> success
