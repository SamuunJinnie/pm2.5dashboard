import pandas as pd
import numpy as np
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

import time
from datetime import datetime 
import os



service = ChromeService(executable_path=ChromeDriverManager().install())
driver = webdriver.Chrome(service=service)


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

def scrapeAllData(lng,lat):
    #PM25 PM10 NO2 SO2 Co O3 + RH TEMP
    pm25 = scrape(f'https://earth.nullschool.net/#current/particulates/surface/level/anim=off/overlay=pm2.5/equirectangular/loc={lng},{lat}')
    pm10 =  scrape(f'https://earth.nullschool.net/#current/particulates/surface/level/anim=off/overlay=pm10/equirectangular/loc={lng},{lat}')
    no2 = scrape(f'https://earth.nullschool.net/#current/chem/surface/level/anim=off/overlay=no2/equirectangular/loc={lng},{lat}')
    co = scrape(f'https://earth.nullschool.net/#current/chem/surface/level/anim=off/overlay=cosc/equirectangular/loc={lng},{lat}')
    so2 = scrape(f'https://earth.nullschool.net/#current/chem/surface/level/anim=off/overlay=so2smass/equirectangular/loc={lng},{lat}')
    # o3 = scrape()
    rh = scrape(f'https://earth.nullschool.net/#current/wind/surface/level/anim=off/overlay=relative_humidity/equirectangular/loc={lng},{lat}')
    temp = scrape(f'https://earth.nullschool.net/#current/wind/surface/level/anim=off/overlay=temp/equirectangular/loc={lng},{lat}')
    return {"datetime_aq": str(datetime.now()),'pm25':pm25,'pm10':pm10,'no2':no2,'co':co,'so2':so2,'rh':rh,'temp':temp}

# print(scrapeAllData(100.141,14.898))
    
def scrapeAllStations() :
    df = pd.read_csv('../../model/prepared_data/others/station_lat_long.csv')
    pm25_col = []
    pm10_col = []
    no2_col = []
    co_col = []
    so2_col = []
    rh_col = []
    temp_col = []
    datetime_col = []
    for ind in df.index :
        data = scrapeAllData(df['longs'][ind], df['lats'][ind])
        pm25_col.append(data['pm25'])
        pm10_col.append(data['pm10'])
        no2_col.append(data['no2'])
        co_col.append(data['co'])
        so2_col.append(data['so2'])
        rh_col.append(data['rh'])
        temp_col.append(data['temp'])
        datetime_col.append(data['datetime_aq'])
    df['pm25'] = pm25_col
    df['pm10'] = pm10_col
    df['no2'] = no2_col
    df['co'] = co_col
    df['so2'] = so2_col
    df['rh'] = rh_col
    df['temp'] = temp_col
    df['datetime_aq'] = datetime_col

    return df