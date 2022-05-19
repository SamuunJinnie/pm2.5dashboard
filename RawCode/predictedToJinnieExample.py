import requests
import pandas as pd
import psycopg2
import random
import json

from datetime import datetime

print('connecting to DB . . .')
conn = psycopg2.connect(
    database='mydb',
    user='puiza',
    password='password'
)
cursor = conn.cursor()
print('connected to DB !')

# --------------------- insert mock data to db ------------------

# for i in range(30):

#     queryStr = 'INSERT INTO predicted_data (device,lat,lng,pm25,datetime_aq) VALUES (%s, %s, %s, %s, %s);'
#     device = random.randint(1000000, 1000000000)
#     lat = random.uniform(1, 100)
#     lng = random.uniform(1, 100)
#     pm25 = random.uniform(1, 100)
#     datetime_aq = datetime.now()

#     data = (device,lat,lng,pm25,datetime_aq)
#     cursor.execute(queryStr, data)
#     conn.commit()

# --------------------- get 30 last puikung(s) from db -----------------

queryStr = 'SELECT * FROM predicted_data ORDER BY id DESC LIMIT 30;'
cursor.execute(queryStr)
conn.commit()

results = cursor.fetchall()

# print('Type : ', type(results))
# print(results)

conn.close()

print('------------------------------ done Query ! -----------------------------------')

# ---------------------- convert data into df --------------------------

df = pd.DataFrame(results, columns=['id', 'device', 'lat', 'lng', 'value', 'date'])
# df = df.drop(['id'], axis='columns')
df['type'] = 'predicted'

print("------------ this is final df --------------")
print(df.tail())

# convert df to json

result = df.to_json(orient="records")
parsed = json.loads(result)
json.dumps(parsed, indent=4)

# print(parsed)

# post to jinnie

url = "https://api.powerbi.com/beta/271d5e7b-1350-4b96-ab84-52dbda4cf40c/datasets/7822ddc3-6b79-4dd6-bc8b-d020325c743c/rows?key=T87n6eQ%2FwSuzoWoRhYzHrzDCkyUrxtBh1rWRj567bbfRaggrlD7cGjrpJN49NfTdXG62RN91x%2FtcaEzy7Reb6Q%3D%3D"

headers = {
  "Content-Type": "application/json"
  }

def push_streaming_data(data):
  response = requests.request(
      method="POST",
      url=url,
      headers=headers,
      data=json.dumps(data)
  )
  return response

# parsed = [
#   {
#     "date" :"2022-05-19T13:22:25.231Z",
#     "value" :98.6,
#     "type" :"predicted",
#     "lat" :98.644432423423423432432,
#     "lng" :98.6444,
#     "device" :"AAAAA555555"
#   }
# ]

print(push_streaming_data(parsed))