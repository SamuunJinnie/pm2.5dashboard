from os import listdir, makedirs
from os.path import isfile, join, exists
import pandas as pd
import numpy as np
import datetime


old_basepath = '../basedata/PCD Data/Data before 2020-9'
new_basepath = '../basedata/PCD Data/Data after 2020-7/PCD data after 2020-7.csv'

# return dict mapping station to path
def get_station_paths(basepath):
    paths = {}
    folders = [basepath]
    for current_dir in folders:
        for item in listdir(current_dir):
            if isfile(join(current_dir, item)):
                if item[0] == '(':
                    end = item.find(')')
                    paths[item[1: end]] = join(current_dir, item)
            else:
                folders.append(join(current_dir, item))
    return paths

# get lat long of all station
def get_lat_long(basepath):
    df = pd.read_csv(basepath)
    mapper = {'stationIDs':[], 'lats':[], 'longs':[]}
    for station in df.stationID.unique():
        lat_long = df[df.stationID == station][['lat', 'long']].mean()
        mapper['stationIDs'].append(station)
        mapper['lats'].append(lat_long['lat'])
        mapper['longs'].append(lat_long['long'])
    return pd.DataFrame(mapper)

def replaceOutlier(df):
    Q1 = df.quantile(0.25)
    Q3 = df.quantile(0.75)
    IQR = Q3 - Q1
    left_boundary = Q1 - 1.5*IQR
    right_boundary = Q3 + 1.5*IQR
    df_no_out = df 
    df_no_out[df.columns] = df.clip(left_boundary,right_boundary,axis=1)
    return df_no_out

def format_datetime_old_data(row):
    date = str(row['ปี/เดือน/วัน'])
    hour = int((row['ชั่วโมง']//100)%24)
    str_datetime = f'20{date[:2]}-{date[2:4]}-{date[4:6]} {str(hour)}:00:00.000000 +0700'
    datetime_obj = datetime.datetime.strptime(str_datetime, '%Y-%m-%d %H:%M:%S.%f %z')
    return datetime_obj

def prepare_old_station_data(station):
    columns = ['ปี/เดือน/วัน', 'ชั่วโมง', 'PM10', 'PM2.5', 'CO', 'NO2', 'O3', 'WS', 'WD', 'TEMP', 'RH']
    path = old_station_paths[station]
    df = pd.read_excel(path, index_col=None, usecols=columns)
    df = df.loc[~df['ปี/เดือน/วัน'].isna()].copy()
    df.dropna(how='all', inplace=True)

    df['datetime'] = df.apply(lambda row: format_datetime_old_data(row), axis=1)
    df.drop(columns=['ปี/เดือน/วัน', 'ชั่วโมง'], inplace=True)
    df.drop_duplicates(inplace=True, subset=['datetime'])
    df.set_index('datetime', inplace=True)

    errors = set()
    for col in df.columns:
        for data in df[col]:
            try:
                float(data)
            except:
                errors.add(data)    
    for err in errors:
        df.replace(err, np.NaN, inplace=True)
    df.astype(float)

    df.interpolate(inplace=True)
    df = df.resample('h').ffill()
    df = replaceOutlier(df)
    return df

# ==================================================================================================
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

print(prepare_old_station_data('76t'))




