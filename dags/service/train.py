import numpy as np
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
from os.path import exists
from tensorflow.keras import Sequential, models
from tensorflow.keras.layers import Dense, LSTM, Dropout
from tensorflow.keras.callbacks import EarlyStopping, ReduceLROnPlateau
from tensorflow.keras.optimizers import Adam
import pandas as pd
import datetime
import joblib
import psycopg2
import psycopg2.extras as extras

model_weight_path = 'best_weight.h5'
scaler_weight_path = 'scaler.gz'

def init_model():
    modelLSTM = Sequential()
    modelLSTM.add(LSTM(units = 24, activation = 'relu', input_shape = (24, 6)))
    modelLSTM.add(Dropout(0.5))
    modelLSTM.add(Dense(units = 24))
    optimizer = Adam(learning_rate=0.0005)
    modelLSTM.compile(optimizer=optimizer, loss = 'mean_squared_error')
    return modelLSTM

if exists(model_weight_path):
    modelLSTM = models.load_model(model_weight_path)
else:
    modelLSTM = init_model()

def train_model(X_train, y_train, epochs=1):
    earlystopping = EarlyStopping(
        monitor='loss', 
        patience=3, 
        min_delta=0, 
        mode='auto'
    )

    reduce_lr = ReduceLROnPlateau(
        monitor='loss', 
        factor=0.2,   
        patience=2, 
        min_lr=0.001,
        verbose=0
    )

    callbacks = [earlystopping, reduce_lr]
    trained = modelLSTM.fit(X_train, y_train, epochs=epochs, batch_size=32, callbacks=callbacks)
    return trained

def prep_data_live(df, scaler):
  df = df[['pm25','temp','rh','pm10', 'lat','long']]
  arr = scaler.transform(df)
  inputs = []
  outputs = []
  for i in range(len(arr)//48):
    iov = arr[i*48:i*48+48]
    input = iov[:24]
    output = iov[24:]
    inputs.append(input)
    outputs.append(output)
  return np.array(inputs), np.array(outputs)

def live_train(modelLSTM, data):  
    data['datetime'] = pd.to_datetime(data['datetime_aq'], format='%Y-%m-%d %H:%M:%S.%f')
    data = data.sort_values(by='datetime')
    df = pd.DataFrame()
    devices = []
    for device in data['device'].unique():
        temp = data[data['device'] == device]
        df = pd.concat([df, temp])
        devices.append(device)
    if exists(scaler_weight_path):
        scaler = joblib.load(scaler_weight_path)
    else:
        scaler = MinMaxScaler()
        scaler.fit(df)
    inputs, outputs = prep_data_live(df, scaler)
    train_model(inputs, outputs)
    y_pred = modelLSTM.predict(outputs)

    cur_date = data['datetime'].iloc[-1]
    hours = []
    for i in range(72):
        cur_date += datetime.timedelta(hours=1)
        hours.append(cur_date)

    to_save_df = pd.DataFrame()
    for i in range(len(devices)):
        cur_data = data[data['device'] == devices[i]]
        cur_pred = y_pred[i].tolist()
        cur_data['datetime'] = hours
        cur_data['pm25'] = cur_pred
        to_save_df = pd.concat([to_save_df, cur_data])

    return to_save_df


# live_train(modelLSTM)

def inputModel():
    conn = psycopg2.connect(database='airflow', user='airflow', password='airflow',host='172.24.0.2')
    cursor = conn.cursor()
    query = 'SELECT * FROM raw_data ORDER BY id DESC LIMIT 49104;'
    cursor.execute(query)
    conn.commit()

    results = cursor.fetchall()
    columns = ['id','device', 'lat', 'lng', 'pm25', 'pm10', 'rh', 'temp', 'datetime_aq']
    df = pd.DataFrame(results,columns=columns)
    prediction = live_train(df)
    insertDFtoDB(conn,prediction,"predicted_data")
    conn.close()



# print(df.to_sql('test2',con=conn,if_exists='append',index=True))
def insertDFtoDB(conn, df, table):
    tuples = [tuple(x) for x in df.to_numpy()]
    cols = ','.join(list(df.columns))
    # SQL query to execute
    query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("the dataframe is inserted")
    cursor.close()
