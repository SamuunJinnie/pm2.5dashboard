import pandas as pd
import numpy as np
import datetime

def prep_3_day_non_correlate_iov(first_valid_datetime, last_valid_datetime, offset):
    start_allset = f'{first_valid_datetime.year}-01-01 00:00:00+07:00'
    start_datetime = pd.to_datetime(start_allset, format='%Y-%m-%d %H:%M:%S%z')  + datetime.timedelta(hours=3*(offset%8))
    while start_datetime < first_valid_datetime:
        start_datetime += datetime.timedelta(hours=72)
    start_of_sets = {}
    for i in range(60):
        start_of_sets[i] = []
    label = 0
    while start_datetime < last_valid_datetime - datetime.timedelta(hours=72):
        start_of_sets[label%60].append(start_datetime)
        start_datetime += datetime.timedelta(hours=72)
        label += 1
    to_use = []
    set_no_list = []
    flags = []
    set_no = 0
    for label in start_of_sets:
        start_of_label = np.random.choice(start_of_sets[label])
        # 8 time step input 8 output 8 validate
        for i in range(24):
            to_use.append(start_of_label + datetime.timedelta(hours=3*i))
            set_no_list.append(set_no)
            if i <8:
                flags.append('I')
            elif i <16:
                flags.append('O')
            else:
                flags.append('V')
        set_no += 1
    return pd.DataFrame({'datetime':to_use, 'set_no':set_no_list, 'flag':flags}) 

def prep_3_day_correlatable_iov(first_valid_datetime, last_valid_datetime, offset):
    start_allset = f'{first_valid_datetime.year}-01-01 00:00:00+07:00'
    start_datetime = pd.to_datetime(start_allset, format='%Y-%m-%d %H:%M:%S%z')  + datetime.timedelta(hours=3*(offset%8))
    while start_datetime < first_valid_datetime:
        start_datetime += datetime.timedelta(hours=24)
    start_of_sets = []
    while start_datetime < last_valid_datetime - datetime.timedelta(hours=72):
        start_of_sets.append(start_datetime)
        start_datetime += datetime.timedelta(hours=24)
    to_use = []
    set_no_list = []
    flags = []
    if len(start_of_sets) > 40:
        start_of_sets = np.random.choice(start_of_sets, size=40)
    set_no = 0
    for starter in  start_of_sets:
        # 8 time step input 8 output 8 validate
        for i in range(24):
            to_use.append(starter + datetime.timedelta(hours=3*i))
            set_no_list.append(set_no)
            if i <8:
                flags.append('I')
            elif i <16:
                flags.append('O')
            else:
                flags.append('V')
        set_no += 1
    return pd.DataFrame({'datetime':to_use, 'set_no':set_no_list, 'flag':flags}) 

def prep_3_day_iov(dt_list, offset):
    first_valid_datetime = dt_list[0]
    last_valid_datetime = dt_list[-1]
    if (last_valid_datetime - first_valid_datetime).days > 540:
        to_use = prep_3_day_non_correlate_iov(first_valid_datetime, last_valid_datetime, offset)
    else:
        to_use = prep_3_day_correlatable_iov(first_valid_datetime, last_valid_datetime, offset)
    return to_use
