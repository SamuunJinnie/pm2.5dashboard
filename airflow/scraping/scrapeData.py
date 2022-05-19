import pandas as pd
import numpy as np
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
import requests
import time
from datetime import datetime 
import os
import json

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

def scrape_longdo(url):
    response = requests.get(url)
    return  json.loads(response.text)['index']

def scrapeAllData(lng,lat):
    #PM25 PM10 NO2 SO2 Co O3 + RH TEMP Traffic
    pm25 = scrape(f'https://earth.nullschool.net/#current/particulates/surface/level/anim=off/overlay=pm2.5/equirectangular/loc={lng},{lat}')
    pm10 =  scrape(f'https://earth.nullschool.net/#current/particulates/surface/level/anim=off/overlay=pm10/equirectangular/loc={lng},{lat}')
    no2 = scrape(f'https://earth.nullschool.net/#current/chem/surface/level/anim=off/overlay=no2/equirectangular/loc={lng},{lat}')
    co = scrape(f'https://earth.nullschool.net/#current/chem/surface/level/anim=off/overlay=cosc/equirectangular/loc={lng},{lat}')
    so2 = scrape(f'https://earth.nullschool.net/#current/chem/surface/level/anim=off/overlay=so2smass/equirectangular/loc={lng},{lat}')
    # o3 = scrape()
    rh = scrape(f'https://earth.nullschool.net/#current/wind/surface/level/anim=off/overlay=relative_humidity/equirectangular/loc={lng},{lat}')
    temp = scrape(f'https://earth.nullschool.net/#current/wind/surface/level/anim=off/overlay=temp/equirectangular/loc={lng},{lat}')
    traffic = scrape_longdo(f'https://traffic.longdo.com/api/json/traffic/index')
    return {"datetime_aq": str(datetime.now()),"data":{'pm25':pm25,'pm10':pm10,'no2':no2,'co':co,'so2':so2,'rh':rh,'temp':temp,'traffic':traffic}}

print(scrapeAllData(100.141,14.898))