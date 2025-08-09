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

element_list = []
chrome_options = webdriver.ChromeOptions()
#chrome_options.add_argument("--headless")  # Run โดย ไม่เปิด browser
#chrome_options.add_argument("--no-sandbox")
#chrome_options.add_argument("--disable-logging")
#chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_experimental_option("detach", True)
driver = webdriver.Chrome(options=chrome_options)

def web_scrape():
    
    url = "https://search.tci-thailand.org/advance_search.html"
    driver.get(url)
    
    search_box = driver.find_element(By.NAME, "keyword[]")
    search_box.send_keys("มหาวิทยาลัยสงขลานครินทร์")
    search_box.send_keys(Keys.ENTER)
    print("==Search performed successfully.===")

    page = 5 #ระบุจำนวนหน้าที่ต้องการ 

    for i in range(page):
        element = WebDriverWait(driver, 100000000).until(EC.visibility_of_element_located((By.ID, "data-article")))
        content = driver.find_elements(By.CSS_SELECTOR, ".col-md-12 .card .content")
        #ส่วนนี้คือการนำข้อมูลมาจัดเก็บ 
        data_processor(content)

        #ส่วนนี้คือการสั่งเก็บข้อมูลในหน้าถัดไป
        next_button_locator = (By.CLASS_NAME, "paginationjs-next")
        next_button = WebDriverWait(driver, 1000000).until(
        EC.element_to_be_clickable(next_button_locator)
        )
        next_button.click()
        time.sleep(3)

        #ส่วนนี้คือการแสดงข้อมูลใน Teminal 
        for row in element_list:
            print("title : ", row[0])
            print("author: ", row[1]) 
            print("year : ",  row[2])
            print("---------------")
    #ส่วนนี้คือการ export ข้อมูลไปเก็บใน Excel   
    excel_export(element_list)

def data_processor(content):
    for i in range(len(content)):
        
        df = str(content[i].text)
        split_df = df.splitlines()
        title = split_df[0]    
        author = ""
        year = ""

        if(len(split_df)<=3):
            year = split_df[2]
            author = split_df[1]
        else:
            year = split_df[3]
            author = split_df[2]

        year = year.split(",")
        if len(year)<=2:
           year = year[0]
        else:
           year = year[2]

        #print("title : "+title)
        #print("author: "+author) 
        #print("year : "+year)
        #print("---------------")
        element_list.append([
            title,
            author,
            year,
        ])

#export ข้อมูลไปที่ excel
def excel_export(element_list):
    df = pd.DataFrame(element_list)
    df.to_excel('output_data_TCI.xlsx', index=False)

web_scrape()




