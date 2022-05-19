from os import listdir
from os.path import isfile, join
import pandas as pd
import numpy as np
import datetime


old_basepath = '../basedata/PCD Data/Data before 2020-9'
old_col_mapper = {'CO': 'CO', ' NO2': 'NO2', ' SO2 ': 'SO2', 'O3': 'O3', ' PM10': 'PM10', ' Wind speed': 'WS', ' Wind dir': 'WD',
    ' Temp': 'Temp', ' PM2.5': 'PM25', 'PM10': 'PM10', 'PM2.5': 'PM25', 'NO2': 'NO2', 'SO2': 'SO2', 
    'WS': 'WS', 'WD': 'WD', 'TEMP': 'Temp', 'RH': 'Rain', 'PM2.5 ': 'PM25', ' CO': 'CO', ' WD': 'WD', ' WS ': 'WS', 'Temp': 'Temp',
    ' TEMP': 'Temp', ' RH': 'Rain', ' CO ': 'CO', ' Rain': 'Rain', 'CO(ppm)': 'CO', 'PM10(มคก./ลบ.ม.)': 'PM10', 'TMP': 'Temp'}
# old_to_drop = ['NO', 'Nox', ' NO ', ' NOX ', ' Glob rad', ' Total HC', 'CH4 (ppm)', 'Pressure', ' Rel hum', ' Pressure']
to_have = ['CO', 'NO2', 'SO2', 'O3', 'PM10', 'WS', 'WD', 'Temp', 'Rain', 'PM25']

new_basepath = '../basedata/PCD Data/Data after 2020-7/PCD data after 2020-7.csv'
new_df_columns = ['stationID', 'PM25', 'PM10', 'NO2', 'SO2', 'CO', 'O3', 'datetime_aq']
new_df = pd.read_csv(new_basepath, usecols=new_df_columns)

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

def replace_outlier(df):
    Q1 = df.quantile(0.25)
    Q3 = df.quantile(0.75)
    IQR = Q3 - Q1
    left_boundary = Q1 - 1.5*IQR
    right_boundary = Q3 + 1.5*IQR
    df_no_out = df 
    df_no_out[df.columns] = df.clip(left_boundary,right_boundary,axis=1)
    return df_no_out

def format_datetime_old_data(row):
    if 'ปี/เดือน/วัน' in row.index:
        date = str(row['ปี/เดือน/วัน'])
        hour = int((row['ชั่วโมง']//100)%24)
        period = '20'
        if int(date[:2]) > 21:
            period = 19
        str_datetime = f'{period}{date[:2]}-{date[2:4]}-{date[4:6]} {str(hour)}:00:00.000000 +0700'
    else:
        date = str(row['วัน/เดือน/ปี'])
        hour = int((row['ชั่วโมง']//100)%24)
        str_datetime = f'20{date[4:6]}-{date[2:4]}-{date[:2]} {str(hour)}:00:00.000000 +0700'
    return datetime.datetime.strptime(str_datetime, '%Y-%m-%d %H:%M:%S.%f %z')

def format_datetime_new_data(dt):
    return datetime.datetime.strptime(f'{dt} +0700', '%Y-%m-%d %H:%M:%S.%f %z')

def prepare_old_station_data(station):
    old_station_paths = get_station_paths(old_basepath)
    path = old_station_paths[station]
    df = pd.read_excel(path, index_col=None).iloc[:, :12]

    if 'ปี/เดือน/วัน' in df.columns: 
        date_in_format = 'ปี/เดือน/วัน'
    else:
        date_in_format = 'วัน/เดือน/ปี'
    df = df.loc[~df[date_in_format].isna()].copy()
    df = df[df[date_in_format].apply(lambda d: d >= 100000)]
    df['datetime'] = df.apply(lambda row: format_datetime_old_data(row), axis=1)
    df.drop(columns=[date_in_format, 'ชั่วโมง'], inplace=True)
    df.drop_duplicates(inplace=True, subset=['datetime'])
    df.set_index('datetime', inplace=True)

    df.rename(columns=old_col_mapper, inplace=True)
    for col in list(set(to_have) - set(df.columns)):
            df[col] = np.NaN
    df = df[to_have]
    # df.drop(old_to_drop, axis=1, inplace=True, errors='ignore')

    errors = set()
    for col in df.columns:
        # drop if there are too small data so the interpolate would grant same data all the columns
        na_count = df[col].isna().sum()
        for data in df[col]:
            try:
                float(data)
            except:
                errors.add(data)    
    for err in errors:
        df.replace(err, np.NaN, inplace=True)
    for col in df.columns:
        na_count = df[col].isna().sum()
        if na_count > 0.3 * len(df):
            df[col] = np.NaN

    df.dropna(axis=1, how='all', inplace=True)
    df.astype(float)
    df = df.loc[(df.isna().sum(axis=1) < 4), :]
    df.interpolate(inplace=True)
    df = df.interpolate().bfill()
    df = df.resample('h').ffill()
    df = replace_outlier(df)
    return df

def prepare_new_station_data(station):
    df = new_df[new_df.stationID == station].copy()
    df['datetime_aq'] = df['datetime_aq'].apply(lambda dt: format_datetime_new_data(dt))
    df.drop(columns=['stationID'], inplace=True)
    df.drop_duplicates(inplace=True, subset=['datetime_aq'])
    df.set_index('datetime_aq', inplace=True)

    for col in df.columns:
        # drop if there are too small data so the interpolate would grant same data all the columns
        na_count = df[col].isna().sum()
        if na_count > 0.3 * len(df):
            df[col] = np.NaN

    df.interpolate(inplace=True)
    df = df.interpolate().bfill()
    df = df.resample('h').ffill()
    df = replace_outlier(df)
    for col in list(set(to_have) - set(df.columns)):
            df[col] = np.NaN
    df = df[to_have]
    return df
