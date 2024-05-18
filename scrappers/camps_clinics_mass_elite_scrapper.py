import os
import sys
sys.path.insert(1,os.path.join(os.path.realpath('__file__').split("lacrosse-prediction")[0],'lacrosse-prediction'))

from time import sleep
from datetime import datetime
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait

from config.config import category_config_dict

configuration = category_config_dict['camps_and_clinics']
CATEGORY_NAME = configuration['category_name']
WEBSOURCE = configuration['websources_list'][1]
OUTPUT_FOLDER_PATH=os.path.join(configuration['scrapper_output_folder_path'],WEBSOURCE)

opt = webdriver.ChromeOptions()
opt.add_argument("--start-maximized")
opt.add_experimental_option("excludeSwitches", ["disable-popup-blocking"])
opt.add_argument("--headless")


WRITE_FLAG=True

output_file_name=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
base_link="https://www.masselite.com/team/college-camps-and-clinics/"

def load_webpage_dynamically(
        weblink:str,
        driver_obj:object,
        sleep_time=2,
        increment=1,
        limit=10
    ):
    """to load webpage recursively"""
    print(f'sleep time: {sleep_time} increment: {increment}')
    print(f"getting {weblink}")
    driver_obj.get(weblink)
    if limit < sleep_time+increment:
        return False
    sleep(sleep_time)
    # check if page is loaded
    if not EC.presence_of_element_located('.//table[@class="tablepress tablepress-id-47 dataTable no-footer"]'):
        return load_webpage_dynamically(weblink=weblink,driver_obj=driver_obj,sleep_time=sleep_time+increment,increment=increment+1,limit=limit)
    return True

def fetch_table_data(driver:object):
    """ fetchs table data from loaded page """
    table_element = WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.XPATH, './/table[@class="tablepress tablepress-id-47 dataTable no-footer"]'))
            )
    df_main =  pd.read_html(table_element.get_attribute("outerHTML"))[0]
    all_links=[]
    rows = table_element.find_elements(by=By.XPATH, value=".//tr")
    # Iterate through each row
    for row in rows:
        # Extract the cells of the current row
        cells = row.find_elements(by=By.XPATH, value=".//td")
        # Check if the row has cells
        if cells:
            # Get the last cell of the row
            last_cell = cells[-1]
            # Check if the last cell contains a link
            link = last_cell.find_element(by=By.XPATH, value=".//a[@href]").get_attribute("href")
            all_links.append(link)
    df_main['CAMP LINK']=all_links
    return df_main

def main():
    """ main execution of scraping bot """
    if (not os.path.exists(OUTPUT_FOLDER_PATH)):
        print(f"{OUTPUT_FOLDER_PATH} not exist")
        return False  
    driver = webdriver.Chrome(options=opt)
    try:
        if load_webpage_dynamically(base_link,driver):
            print("featching table data now ... ")
            output_df=fetch_table_data(driver)
            if WRITE_FLAG:
                output_df.to_csv(f"{OUTPUT_FOLDER_PATH}/{output_file_name}.csv")
                print(f'completed,\n{output_df.shape} record found, \n {output_file_name} File saved successfully...')
                return True

    except Exception as e:
        print(f"fetch data fail, exception accured")
        print(e)
        driver.close()
        return False
    driver.close()

if __name__ =="__main__":
    if main():
        print("success")
    else:
        print("Fail")
