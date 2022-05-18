from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
service = ChromeService(executable_path=ChromeDriverManager().install())
driver = webdriver.Chrome(service=service)
import time

def scrape(url):
    # url = f'https://earth.nullschool.net/chem/surface/level/anim=off/overlay=so2smass/equirectangular/loc={lng},{lat}'
    #go to web/#current
    driver.get(url=url)
    element = WebDriverWait(driver,9999).until(EC.visibility_of_element_located((By.XPATH, '//*[@id="spotlight-panel"]/div[3]/div')))
    data_status = driver.find_element(By.XPATH,'/html/body/main/div[3]/div[1]/div')
    if data_status.text=="Downloading...":
        while True:
            time.sleep(0.05)
            data_status = driver.find_element(By.XPATH,'/html/body/main/div[3]/div[1]/div')
            if data_status.text=="Downloading...":
                continue
            else :
                break
    #so2
    data = element.text.split(' ')[0]
    return data

from os import makedirs
from os.path import join
import pandas as pd
import pytz

station_lat_long = pd.read_csv('prepared_data/others/station_lat_long.csv', usecols=['stationIDs', 'lats', 'longs'])
data_base_path = 'prepared_data/stations'
to_use_base_path = 'prepared_data/to_uses'
scraped_base_path = 'prepared_data/scraped'
makedirs(scraped_base_path, exist_ok=True)
start = int(input('start : '))
stop = int(input('stop : '))
for i in range(start, stop):
    cur_station = station_lat_long.iloc[i]
    fn = f'{cur_station.stationIDs}.csv'
    df = pd.read_csv(join(data_base_path, fn)).set_index('datetime')
    to_use = pd.read_csv(join(to_use_base_path, fn)).set_index('datetime')
    df = df.loc[to_use.index]
    df['set_no'] = to_use['set_no']
    df.index = pd.to_datetime(df.index , format='%Y-%m-%d %H:%M:%S%z')
    for datetime in df.index:
        datetime_utc = datetime.astimezone(pytz.utc)
        lat = station_lat_long.iloc[i]['lats']
        lng = station_lat_long.iloc[i]['longs']
        year = datetime_utc.year
        month = datetime_utc.month
        day = datetime_utc.day
        hour = datetime_utc.hour
        urls = {
            'PM25':f'https://earth.nullschool.net/#{year}/{month}/{day}/{hour}00Z/particulates/surface/level/anim=off/overlay=pm2.5/equirectangular/loc={lng},{lat}',
            'PM10':f'https://earth.nullschool.net/#{year}/{month}/{day}/{hour}00Z/particulates/surface/level/anim=off/overlay=pm10/equirectangular/loc={lng},{lat}',
            'NO2':f'https://earth.nullschool.net/#{year}/{month}/{day}/{hour}00Z/chem/surface/level/anim=off/overlay=no2/equirectangular/loc={lng},{lat}',
            'SO2':f'https://earth.nullschool.net/#{year}/{month}/{day}/{hour}00Z/chem/surface/level/anim=off/overlay=so2smass/equirectangular/loc={lng},{lat}',
            'CO':f'https://earth.nullschool.net/#{year}/{month}/{day}/{hour}00Z/chem/surface/level/anim=off/overlay=cosc/equirectangular/loc={lng},{lat}',
            'Rain':f'https://earth.nullschool.net/#{year}/{month}/{day}/{hour}00Z/wind/surface/level/anim=off/overlay=relative_humidity/equirectangular/loc={lng},{lat}',
            'Temp':f'https://earth.nullschool.net/#{year}/{month}/{day}/{hour}00Z/wind/surface/level/anim=off/overlay=temp/equirectangular/loc={lng},{lat}'
        }
        cur_row = df.loc[datetime]
        na_checker = cur_row.isna()
        for col in cur_row.index:
            if na_checker[col] and col in urls:
                data = scrape(urls[col])
                df.loc[datetime, col] = data
    df.to_csv(join(scraped_base_path, fn))
                
            
    
    

    