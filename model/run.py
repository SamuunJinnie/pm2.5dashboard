from os import  makedirs
from os.path import join, exists
import pandas as pd
import numpy as np
import datetime
from prepare_data import get_station_paths, get_lat_long, prepare_old_station_data, prepare_new_station_data
from path_finder import prep_3_day_iov


old_basepath = '../basedata/PCD Data/Data before 2020-9'
old_col_mapper = {'CO': 'CO', ' NO2': 'NO2', ' SO2 ': 'SO2', 'O3': 'O3', ' PM10': 'PM10', ' Wind speed': 'WS', ' Wind dir': 'WD',
    ' Temp': 'Temp', ' PM2.5': 'PM25', 'PM10': 'PM10', 'PM2.5': 'PM25', 'NO2': 'NO2', 'SO2': 'SO2', 
    'WS': 'WS', 'WD': 'WD', 'TEMP': 'Temp', 'RH': 'Rain', 'PM2.5 ': 'PM25', ' CO': 'CO', ' WD': 'WD', ' WS ': 'WS', 'Temp': 'Temp',
    ' TEMP': 'Temp', ' RH': 'Rain', ' CO ': 'CO', ' Rain': 'Rain', 'CO(ppm)': 'CO', 'PM10(มคก./ลบ.ม.)': 'PM10', 'TMP': 'Temp'}
# old_to_drop = ['NO', 'Nox', ' NO ', ' NOX ', ' Glob rad', ' Total HC', 'CH4 (ppm)', 'Pressure', ' Rel hum', ' Pressure']
old_to_have = ['CO', 'NO2', 'SO2', 'O3', 'PM10', 'WS', 'WD', 'Temp', 'Rain', 'PM25']

new_basepath = '../basedata/PCD Data/Data after 2020-7/PCD data after 2020-7.csv'
new_df_columns = ['stationID', 'PM25', 'PM10', 'NO2', 'SO2', 'CO', 'O3', 'datetime_aq']
new_df = pd.read_csv(new_basepath, usecols=new_df_columns)



makedirs('prepared_data', exist_ok=True)
makedirs('prepared_data/others', exist_ok=True)
station_lat_long_path = 'prepared_data/others/station_lat_long.csv'
if not exists(station_lat_long_path):
    station_lat_long = get_lat_long('../basedata/PCD Data/Data after 2020-7/PCD data after 2020-7.csv')
    station_lat_long.to_csv(station_lat_long_path)

old_station_path_path = 'prepared_data/others/old_station_path.csv'
old_station_paths = get_station_paths(old_basepath)
if not exists(old_station_path_path):
    old_station_paths_df = pd.DataFrame(old_station_paths.items(), columns=['station', 'path'])
    old_station_paths_df.to_csv(old_station_path_path)


makedirs('prepared_data/stations', exist_ok=True)
to_use_base_path = 'prepared_data/to_uses'
makedirs(to_use_base_path, exist_ok=True)

prepared_data_each_station_path = 'prepared_data/stations/'
i = 0
for station in new_df['stationID'].unique():
    cur_station_path = join(prepared_data_each_station_path, f'{station}.csv')
    try:
        if not exists(cur_station_path):
            df = prepare_new_station_data(station)
            to_use = prep_3_day_iov(df.index, i)
            if station in old_station_paths:
                old_df = prepare_old_station_data(station)
                df = pd.concat([old_df, df]).reset_index().rename(columns={'index':'datetime'}).drop_duplicates(subset=['datetime']).set_index('datetime')
                old_to_use = prep_3_day_iov(old_df.index, i)
                to_use = pd.concat([to_use.iloc[:480], old_to_use.iloc[-480:]])
            else:
                df.reset_index(inplace=True)
                df.rename(columns={'datetime_aq':'datetime'}, inplace=True)
                df.set_index('datetime', inplace=True)
            to_use.to_csv(join(to_use_base_path, f'{station}.csv'))
            df.to_csv(cur_station_path)
            print(i, ':', station, ': complete')
            i += 1
    except:
        print(station, ': failed')
        break
