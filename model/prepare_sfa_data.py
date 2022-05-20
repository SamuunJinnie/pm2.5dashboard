from genericpath import exists
import pandas as pd
from os import makedirs
from os.path import join

sfa = pd.read_csv('../basedata/SFA Data /ID_DATA_SFA.csv')[['device_id', 'time_aq', 'temp', 'humid', 'pm25_corrected', 'pm10_corrected']]
sf = sfa[sfa.isna().sum(axis=1) == 0]
dt_col = pd.to_datetime(sfa['time_aq']+' +0700', format='%Y-%m-%d %H:%M:%S.%f %z').copy()
sfa['datetime'] = dt_col.dt.round('H')

sfa_station_base_path = 'prepared_data/sfa_devices'
makedirs(sfa_station_base_path)
i = 0
for device in sfa['device_id'].unique():
    fn = f'{device}.csv'
    if not exists(join(sfa_station_base_path, fn)):
        cur = sfa[sfa['device_id'] == device].groupby('datetime').mean()
        count = len(cur)
        if count >= 216:
            # 6 day up
            cur.to_csv(join(sfa_station_base_path, fn))
            print(i, ':', fn)
    i += 1


sfa_info = pd.read_csv('../basedata/SFA Data /ID_INFO_SFA.csv')
sfa_info = sfa_info[sfa_info.isna().sum(axis=1) == 0]
df = pd.DataFrame()
devices = []
lats = []
longs = []

for i, row in sfa_info.iterrows():
    device = row['device_id']
    fn = f'{device}.csv'
    if exists(join(sfa_station_base_path, fn)):
        temp = pd.read_csv(join(sfa_station_base_path, fn))
        na = (temp.isna().sum(axis=1) > 0).sum()
        if len(temp) > 216 and na <= 0.3 * len(temp):
            temp = temp.iloc[:(len(temp)//3)*3]
            temp['device'] = row['name_en']
            temp['lat'] = row['lat']
            temp['long'] = row['long']
            devices.append(device)
            lats.append(row['lat'])
            longs.append(row['long'])
            temp.interpolate(inplace=True)
            temp = temp.interpolate().bfill()
            if temp.isna().sum().sum() == 0:
                df = pd.concat([df, temp])
            print(i, ':', fn)

df.rename(columns={'time_aq':'datetime', 'humid':'rh', 'pm25_corrected':'pm25', 'pm10_corrected':'pm10'}, inplace=True)
# order 'pm25', 'datetime','temp','rh','pm10','name','lat','long'

set_size = 24*9*10
for i in range(10):
    cur = 0 + i * set_size
    temp = pd.DataFrame()
    while cur < len(df):
        temp = pd.concat([temp, df[cur:cur+24*9]])
        cur += set_size
    temp.to_csv(f'prepared_data/sfa_partitions/dataset2_{i}.csv')


# df.to_csv('prepared_data/dataset2.csv')
pd.DataFrame({'device':devices, 'lat':lats, 'long':longs}).to_csv('prepared_data/others/device_lat_long.csv')
print(df.isna().sum())


