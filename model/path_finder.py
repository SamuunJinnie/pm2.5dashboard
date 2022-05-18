from os import listdir, makedirs
from os.path import isfile, join, exists
import pandas as pd
import numpy as np
import random
import datetime

data_base_path = 'prepared_data/stations'
to_use_base_path = 'prepared_data/to_uses'
makedirs(to_use_base_path, exist_ok=True)
offset = 0
stations_to_uses = {}
for station in listdir(data_base_path):
    try:
        cur_path = join(to_use_base_path, station)
        if exists(cur_path):
            pass
        df = pd.read_csv(join(data_base_path, station))
        dt_list = pd.to_datetime(df['datetime'] , format='%Y-%m-%d %H:%M:%S%z')
        first_valid_datetime = dt_list.iloc[0]
        last_valid_datetime = dt_list.iloc[-1]
        start_allset = f'{first_valid_datetime.year}-01-01 00:00:00+07:00'
        start_datetime = pd.to_datetime(start_allset, format='%Y-%m-%d %H:%M:%S%z')  + datetime.timedelta(hours=(offset%8))
        while start_datetime < first_valid_datetime:
            start_datetime += datetime.timedelta(hours=9)
        start_of_sets = {}
        for i in range(973):
            start_of_sets[i] = []
        label = 0
        while start_datetime < last_valid_datetime - datetime.timedelta(hours=9):
            start_of_sets[label%973].append(start_datetime)
            start_datetime += datetime.timedelta(hours=9)
            label += 1
        to_use = []
        for label in start_of_sets:
            start_of_label = random.choice(start_of_sets[label])
            for i in range(9):
                to_use.append(start_of_label + datetime.timedelta(hours=3*i))
        stations_to_uses[station] = to_use
        pd.Series(to_use).to_csv(cur_path)
        print(offset, ':', station, ': complete')
        offset += 1
    except:
        print(station, ': failed')
    







