import requests
import pandas as pd
import psycopg2
import random
import json

from datetime import datetime

conn = psycopg2.connect(database='airflow', user='airflow', password='airflow',host='172.24.0.2')
cursor = conn.cursor()
queryStr = 'SELECT * FROM raw_data;'
cursor.execute(queryStr)
results = cursor.fetchall()
conn.commit()

df = pd.DataFrame(results,columns=['id','device','lat','lng','pm25','pm10','rh','temp','datetime_aq'])
print(df.head)

df.to_csv('dump_file.csv')

conn.close()
