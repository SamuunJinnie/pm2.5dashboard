import numpy as np
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import mean_squared_error
from os.path import exists
from tensorflow.keras import Sequential, models
from tensorflow.keras.layers import Dense, LSTM, Dropout
from tensorflow.keras.callbacks import EarlyStopping, ReduceLROnPlateau
from tensorflow.keras.optimizers import Adam
import pandas as pd
import datetime
import joblib

model_weight_path = 'best_weight.h5'
scaler_weight_path = 'scaler.gz'

def init_model():
    modelLSTM = Sequential()
    modelLSTM.add(LSTM(units = 24, activation = 'relu', input_shape = (24, 6)))
    modelLSTM.add(Dropout(0.5))
    modelLSTM.add(Dense(units = 24))

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

    optimizer = Adam(learning_rate=0.0005)
    modelLSTM.compile(optimizer=optimizer, loss = 'mean_squared_error', callbacks=callbacks)
    return modelLSTM

if exists(model_weight_path):
    modelLSTM = models.load_model(model_weight_path)
else:
    modelLSTM = init_model()
   

def train_model(X_train, y_train, epochs=1):
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

def live_train(data):  
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


