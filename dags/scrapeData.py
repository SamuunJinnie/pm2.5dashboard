import pandas as pd
import numpy as np
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
# import chromedriver_autoinstaller
import time
import psycopg2
from datetime import datetime,timezone 
import pytz
import os


def scrape(url, driver):
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

def scrapeAllData(stationID,lng,lat,year,month,day,hour,driver,cursor):
    # url = f'https://earth.nullschool.net/#{year}/{month:02d}/{day:02d}/{hour:02d}00Z/chem/surface/level/overlay=so2smass/equirectangular/loc={lng},{lat}'
    # y,m,d,h : UTC+0 that use for scrape in nullschool
    #PM25 PM10 NO2 SO2 Co O3 + RH TEMP
    time_utc0 = datetime(int(year),int(month),int(day),int(hour), tzinfo=timezone.utc)
    time_utc7 = time_utc0.replace(tzinfo=timezone.utc).astimezone(tz=pytz.timezone('Asia/Bangkok'))
    pm25 = scrape(f'https://earth.nullschool.net/#{year}/{month}/{day}/{hour}00Z/particulates/surface/level/anim=off/overlay=pm2.5/equirectangular/loc={lng},{lat}', driver)
    pm10 =  scrape(f'https://earth.nullschool.net/#{year}/{month}/{day}/{hour}00Z/particulates/surface/level/anim=off/overlay=pm10/equirectangular/loc={lng},{lat}', driver)
    no2 = scrape(f'https://earth.nullschool.net/#{year}/{month}/{day}/{hour}00Z/chem/surface/level/anim=off/overlay=no2/equirectangular/loc={lng},{lat}', driver)
    co = scrape(f'https://earth.nullschool.net/#{year}/{month}/{day}/{hour}00Z/chem/surface/level/anim=off/overlay=cosc/equirectangular/loc={lng},{lat}', driver)
    so2 = scrape(f'https://earth.nullschool.net/#{year}/{month}/{day}/{hour}00Z/chem/surface/level/anim=off/overlay=so2smass/equirectangular/loc={lng},{lat}', driver)
    # o3 = scrape()
    rh = scrape(f'https://earth.nullschool.net/#{year}/{month}/{day}/{hour}00Z/wind/surface/level/anim=off/overlay=relative_humidity/equirectangular/loc={lng},{lat}', driver)
    temp = scrape(f'https://earth.nullschool.net/#{year}/{month}/{day}/{hour}00Z/wind/surface/level/anim=off/overlay=temp/equirectangular/loc={lng},{lat}', driver)
    query = "INSERT INTO raw_data (stationID,lat,lng,pm25,pm10,no2,so2,co,rh,temp,datetime_aq) VALUES (%s, %s, %s, %s, %s,%s,%s,%s,%s,%s,%s);"
    data = (stationID,lat,lng,pm25,pm10,no2,so2,co,rh,temp,str(time_utc7))
    cursor.execute(query, data)
    print('----------------------------------------')
    # return {"datetime_aq": str(time_utc7),'pm25':pm25,'pm10':pm10,'no2':no2,'co':co,'so2':so2,'rh':rh,'temp':temp}
    return

def scrapeAllStations(datetime):
    conn,cursor =connectDB()
    # chromedriver_autoinstaller.install()
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')

    service = ChromeService(executable_path=ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service,options = chrome_options)

    df = pd.read_csv('/opt/airflow/dags/station_lat_long.csv')
    # pm25_col = []
    # pm10_col = []
    # no2_col = []
    # co_col = []
    # so2_col = []
    # rh_col = []
    # temp_col = []
    # datetime_col = []
    temp1, temp2 = datetime.split("T")
    year,month,day = temp1.split("-")
    temp3 = temp2.split(":")
    hour = temp3[0]
    for ind in df.index :
        print("index : ",ind)
        data = scrapeAllData(df['stationIDs'][ind],df['longs'][ind], df['lats'][ind],year,month,day,hour,driver,cursor)
        # pm25_col.append(data['pm25'])
        # pm10_col.append(data['pm10'])
        # no2_col.append(data['no2'])
        # co_col.append(data['co'])
        # so2_col.append(data['so2'])
        # rh_col.append(data['rh'])
        # temp_col.append(data['temp'])
        # datetime_col.append(data['datetime_aq'])
    # df['pm25'] = pm25_col
    # df['pm10'] = pm10_col
    # df['no2'] = no2_col
    # df['co'] = co_col
    # df['so2'] = so2_col
    # df['rh'] = rh_col
    # df['temp'] = temp_col
    # df['datetime_aq'] = datetime_col
    conn.commit()
    driver.quit()
    return

def connectDB():
    print('------------- Connect Database -------------')
    conn = psycopg2.connect(database='airflow', user='airflow', password='airflow',host='172.24.0.2')
    cursor = conn.cursor()
    print('------------- conn -------------')
    print(conn)
    print('------------- cursor -------------')
    print(cursor)
    print('------------- Suscessfully -------------')
    return conn,cursor

