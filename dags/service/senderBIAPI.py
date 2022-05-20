import time
import requests
import pandas as pd
import psycopg2
import random
import json

from datetime import datetime


def sendToBI(typeS):
  print('connecting to DB . . .')
  conn = psycopg2.connect(database='airflow', user='airflow', password='airflow',host='172.24.0.2')
  cursor = conn.cursor()
  print('connected to DB !')

  if typeS == 'history':
    queryStr = 'SELECT * FROM raw_data ORDER BY id DESC LIMIT 341;'
    columns = ['id', 'device', 'lat', 'lng', 'pm25', 'pm10', 'rh', 'temp', 'datetime_aq']

  elif typeS == 'predicted':
    #**********************************************************
    queryStr = 'SELECT * FROM predicted_data ORDER BY id DESC LIMIT 8184;'
    columns = ['id', 'device', 'lat', 'lng', 'value', 'date']

  cursor.execute(queryStr)
  conn.commit()

  results = cursor.fetchall()

  conn.close()

  print('------------------------------ done Query ! -----------------------------------')


  df = pd.DataFrame(results,columns=columns)
  df['type'] = typeS

  if typeS == 'history':
    df.drop([ "pm10", "rh", "temp"], axis='columns',inplace=True)

    name_mapper = {
        'pm25': 'value',
        'datetime_aq': 'date'
    }
    df.rename(columns=name_mapper,inplace=True)

  # ------------------- start adding device name -------------------
  df_with_device_name = pd.read_csv('/opt/airflow/dags/service/ID_INFO_SFA.csv')
  df_with_device_name = df_with_device_name.rename({'device_id': 'device'}, axis='columns')

  # left join and change "name_en" ---> "device"
  df = pd.merge(df,df_with_device_name[['device', 'name_en']],on='device',how='left')
  df = df.drop('device', axis='columns')
  df = df.rename({'name_en': 'device'}, axis='columns')


  result = df.to_json(orient="records")
  parsed = json.loads(result)
  json.dumps(parsed, indent=4)

  url = "https://api.powerbi.com/beta/271d5e7b-1350-4b96-ab84-52dbda4cf40c/datasets/7822ddc3-6b79-4dd6-bc8b-d020325c743c/rows?key=T87n6eQ%2FwSuzoWoRhYzHrzDCkyUrxtBh1rWRj567bbfRaggrlD7cGjrpJN49NfTdXG62RN91x%2FtcaEzy7Reb6Q%3D%3D"

  headers = {
    "Content-Type": "application/json"
    }

  def push_streaming_data(data,count):
    print('------------------ Trying -----------------------')
    count+=1
    print('trials : ', count)
    if(count >= 10):
      return
    response = requests.request(
        method="POST",
        url=url,
        headers=headers,
        data=json.dumps(data)
    )
    if response.status_code != 200:
      print(response.status_code,response.text)
      # time.sleep(30)
      push_streaming_data(data,count)
    print('------------------ Success -----------------------')         
    return response

  print(push_streaming_data(parsed,0))



def sendToBI2(typeS):
  print('connecting to DB . . .')
  conn = psycopg2.connect(database='airflow', user='airflow', password='airflow',host='172.24.0.2')
  cursor = conn.cursor()
  print('connected to DB !')

  if typeS == 'history':
    queryStr = 'SELECT * FROM raw_data ORDER BY id DESC LIMIT 341;'
    columns = ['id', 'device', 'lat', 'lng', 'pm25', 'pm10', 'rh', 'temp', 'datetime_aq']

  elif typeS == 'predicted':
    #**********************************************************
    queryStr = 'SELECT * FROM predicted_data ORDER BY id DESC LIMIT 8184;'
    columns = ['id', 'device', 'lat', 'lng', 'value', 'date']

  cursor.execute(queryStr)
  conn.commit()

  results = cursor.fetchall()

  conn.close()

  print('------------------------------ done Query ! -----------------------------------')


  df = pd.DataFrame(results,columns=columns)
  df['type'] = typeS

  if typeS == 'history':
    df.drop([ "pm10", "rh", "temp"], axis='columns',inplace=True)

    name_mapper = {
        'pm25': 'value',
        'datetime_aq': 'date'
    }
    df.rename(columns=name_mapper,inplace=True)

  # ------------------- start adding device name -------------------
  df_with_device_name = pd.read_csv('/opt/airflow/dags/service/ID_INFO_SFA.csv')
  df_with_device_name = df_with_device_name.rename({'device_id': 'device'}, axis='columns')

  # left join and change "name_en" ---> "device"
  df = pd.merge(df,df_with_device_name[['device', 'name_en']],on='device',how='left')
  df = df.drop('device', axis='columns')
  df = df.rename({'name_en': 'device'}, axis='columns')


  result = df.to_json(orient="records")
  parsed = json.loads(result)
  json.dumps(parsed, indent=4)

  url = "https://api.powerbi.com/beta/271d5e7b-1350-4b96-ab84-52dbda4cf40c/datasets/13bc3203-b5d8-4f4a-acc9-f87f570adc0b/rows?key=L9hFXv6c5G3VfLjFF0dFSJs6TtySEYD3uZFoSp%2F12QmLMahbYJs7U60lTQyMJDeHJt0gtyw64vvLpuqjouMfag%3D%3D"

  headers = {
    "Content-Type": "application/json"
    }

  def push_streaming_data(data,count):
    print('------------------ Trying -----------------------')
    count+=1
    print('trials : ', count)
    if(count >= 10):
      return
    response = requests.request(
        method="POST",
        url=url,
        headers=headers,
        data=json.dumps(data)
    )
    if response.status_code != 200:
      print(response.status_code,response.text)
      # time.sleep(30)
      push_streaming_data(data,count)
    print('------------------ Success -----------------------')         
    return response

  print(push_streaming_data(parsed,0))
