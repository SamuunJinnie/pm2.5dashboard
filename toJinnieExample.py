import requests
import pandas as pd
import psycopg2
import random
import json

print('connecting to DB . . .')
conn = psycopg2.connect(
    database='mydb',
    user='puiza',
    password='password'
)
cursor = conn.cursor()
print('connected to DB !')

# insert puikung(s) to db ------------------

# for i in range(50):

#     queryStr = 'INSERT INTO raw_data (device,lat,lng,pm25,pm10,rh,temp,traffic,datetime_aq) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);'
#     device = random.randint(1000000, 1000000000)
#     lat = random.uniform(1, 100)
#     lng = random.uniform(1, 100)
#     pm25 = random.uniform(1, 100)
#     pm10 = random.uniform(1, 100)
#     rh = random.uniform(1, 100)
#     temp = random.uniform(1, 100)
#     traffic = random.uniform(1, 10)
#     datetime_aq = "2000-08-31T12:00:000"

#     data = (device,lat,lng,pm25,pm10,rh,temp,traffic,datetime_aq)
#     cursor.execute(queryStr, data)
#     conn.commit()


# get puikung(s) from db -----------------

queryStr = 'SELECT * FROM raw_data;'
cursor.execute(queryStr)
conn.commit()

results = cursor.fetchall()

# print('Type : ', type(results))
# print(results)

conn.close()

print('------------------------------ done Query ! -----------------------------------')

# convert data queried from db to dataframe ---------------------------------------
# device,lat,lng,pm25,pm10,rh,temp,traffic,datetime_aq

df = pd.DataFrame(results, columns=['id', 'device', 'lat', 'lng', 'pm25', 'pm10', 'rh', 'temp', 'traffic', 'datetime_aq'])
print(df.head())
print(df.shape)
print(df.describe(include='all'))

#trim df ---------------------------------------------

df_trimmed = df.drop(["id", "pm10", "rh", "temp", "traffic"], axis='columns')
print(df_trimmed.head())

name_mapper = {
    'pm25': 'value',
    'datetime_aq': 'date'
}
df_trimmed = df_trimmed.rename(columns=name_mapper)
df_trimmed['type'] = 'history'

print('---------- after alter df --------')
print (df_trimmed.head())



# convert df to json

result = df_trimmed.tail(30).to_json(orient="records")
parsed = json.loads(result)
json.dumps(parsed, indent=4)

# print(parsed)

# post to jinnie

url = "https://api.powerbi.com/beta/271d5e7b-1350-4b96-ab84-52dbda4cf40c/datasets/e63a7fed-c2ee-4b88-9cc3-8dadb01d642e/rows?key=VDUYAE2tjjrmz2gGpUUh5nIVgpAT8JgTajd9a2TdBJ%2FrEpamPm3PnqagSkZsy92CwX36Ew7JNLFPmQGQmhhylg%3D%3D"

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

print(push_streaming_data(parsed))






