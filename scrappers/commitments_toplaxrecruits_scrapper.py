""" this module contains scrapping bot to scrap data from toplaxrecruits.com """
import os 
import sys
sys.path.insert(1,os.path.join(os.path.realpath('__file__').split("lacrosse-prediction")[0],'lacrosse-prediction'))
from time import sleep

import pandas as pd

from selenium import webdriver
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select


from config.config import category_config_dict
configuration = category_config_dict['commitments']
CATEGORY_NAME = configuration['category_name']
WEBSOURCE = configuration['websources_list'][3]
OUTPUT_FOLDER_PATH=os.path.join(configuration['scrapper_output_folder_path'],WEBSOURCE)

SCRAPE_LINK="https://toplaxrecruits.com/"

chrome_options = Options()
chrome_options.add_argument("--start-maximized")

def main():
    """ main method to run scrapper """
    if not os.path.exists(OUTPUT_FOLDER_PATH):
        print(f"{OUTPUT_FOLDER_PATH} not exist")
        return False
    else:
        driver = webdriver.Chrome(options=chrome_options)
        print(f"Getting {SCRAPE_LINK}")
        driver.get(SCRAPE_LINK)
        sleep(2)
        menu_items=["menu-item-105016","menu-item-105007"]

        categories={}

        print("getting all the sub menus for girls and boys commitments")
        for menu in menu_items:
            
            comitments = driver.find_element(By.XPATH,f'.//li[@id="{menu}"]')
            # Create an ActionChains object
            actions = ActionChains(driver)

            # Hover over the element
            actions.move_to_element(comitments).perform()
            sleep(1)
            sub_list=comitments.find_elements(By.XPATH,".//li")
            len(sub_list)
            for sub_menu in sub_list:
                print(sub_menu.text)
                href_value = sub_menu.find_element(By.XPATH,".//a").get_attribute("href")
                print(href_value)

                categories[sub_menu.text]=href_value

        for cat in categories.keys():
            print(f"scrapping data for {cat}\n")
            driver.get(categories[cat])
            sleep(2)
            print("show 100 records per page\n")
            select = Select(driver.find_element(By.XPATH,'.//select'))

            select.select_by_value("100")
            sleep(2)
            dfs_list=[]
            print("scrolling through all the pages....\n")
            while True:
                sleep(3)

                driver.find_element(By.XPATH,'.//table')

                results=driver.find_elements(By.XPATH,'.//table')
                table_html = results[-1].get_attribute("outerHTML")

                # Use Pandas to read HTML and convert it to a DataFrame
                df = pd.read_html(table_html)[0]
                df = df.dropna(axis = 0, how = 'all')
                if df.shape[0] == 0:
                    break

                try:
                    df.drop(['Unnamed: 8'], axis = 1, inplace = True) 
                except Exception:
                    pass

                dfs_list.append(df)
                try:
                    driver.find_element(By.XPATH,'.//a[@class="paginate_button next"]').click()

                except Exception:
                    break

            result_df=pd.concat(dfs_list, axis=0, ignore_index=True)
            print(f"{result_df.shape} records found\n")
            result_df.to_csv(f"{OUTPUT_FOLDER_PATH}/{cat.replace(' ','_').lower()}.csv", index =False)
            print("csv SAVED")
            return True

if __name__ == "__main__":
    if main():
        print("success")
    else:
        print("fail")
