import requests
import time
import pandas as pd
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


chrome_options = webdriver.ChromeOptions()
#chrome_options.add_argument("--headless")  # เปิดเพื่อ run โดยไม่ open browser 
#chrome_options.add_argument("--no-sandbox")
#chrome_options.add_argument("--disable-logging")
#chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_experimental_option("detach", True)
driver = webdriver.Chrome(options=chrome_options)

def sources():
    element_list = []
    url = "https://openalex.org/sources?filter=default.search:Prince+of+Songkla+University"
    driver.get(url)
    titles = driver.find_elements(By.CLASS_NAME, "v-list-item-title")
    subtitle = driver.find_elements(By.CLASS_NAME, "v-list-item-subtitle")

    for i in range(len(titles)):
        element_list.append([
            titles[i].text,
            subtitle[i].text,
        ])
    print("====sources=====")
    for row in element_list:
        print(row)   
    
    excel_export(element_list,"source")

def authors():
    element_list = []
    url = "https://openalex.org/authors?filter=default.search:Prince+of+Songkla+University"
    driver.get(url)
    
    titles = driver.find_elements(By.CLASS_NAME, "v-list-item-title")
    subtitle = driver.find_elements(By.CLASS_NAME, "v-list-item-subtitle")
    
    for i in range(len(titles)):
        element_list.append([
            titles[i].text,
            subtitle[i].text,
        ])
    print("====authors=====")
    for row in element_list:
        print(row)   

    excel_export(element_list,"authors")

def works():
    element_list = []
    url = "https://openalex.org/works?filter=default.search:Prince+of+Songkla+University"
    driver.get(url)
    
    titles = driver.find_elements(By.CLASS_NAME, "v-list-item-title")
    subtitle = driver.find_elements(By.CLASS_NAME, "v-list-item-subtitle")
    
    for i in range(len(titles)):
        element_list.append([
            titles[i].text,
            subtitle[i].text,
        ])
    print("====works=====")
    for row in element_list:
        print(row)   

    excel_export(element_list,"works")

def excel_export(element_list,name):
    df = pd.DataFrame(element_list)
    names = name+".xlsx"
    df.to_excel(names, index=False)
    print("==Export Excel Success.===",names)


#เพื่อให้ข้อมูลถูกต้อง ควร run ทีละ function 

sources()
#authors()
#works()