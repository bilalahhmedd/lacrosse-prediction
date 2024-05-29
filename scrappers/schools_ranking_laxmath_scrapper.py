""" scrapper for school rankings laxmath """

import os
import sys
sys.path.insert(1,os.path.join(os.path.realpath('__file__').split("lacrosse-prediction")[0],'lacrosse-prediction'))

from time import sleep
import pandas as pd
from selenium import webdriver
import chromedriver_autoinstaller
chromedriver_autoinstaller.install()
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
from selenium.common.exceptions import TimeoutException

from config.config import category_config_dict

configuration = category_config_dict['schools_ranking']
CATEGORY_NAME = configuration['category_name']
WEBSOURCE = configuration['websources_list'][1]
OUTPUT_FOLDER_PATH=os.path.join(configuration['scrapper_output_folder_path'],WEBSOURCE)

opt = webdriver.ChromeOptions()
opt.add_argument("--start-maximized")
opt.add_argument("--headless")
opt.add_experimental_option("excludeSwitches", ["disable-popup-blocking"])



def get_page_table(driver,last_df):
    "this method will get all the entries on the page and return table as dataframe"
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    sleep(2)
    while True:
        try:
            table_element = WebDriverWait(driver, 60).until(
                        EC.presence_of_element_located((By.XPATH, './/table[@id="dtBasicExample"]'))
                    )
            table_html = table_element.get_attribute("outerHTML")
            # Use Pandas to read HTML and convert it to a DataFrame
            df = pd.read_html(table_html)[0]
        except TimeoutException:
            print ("oppss!\ntable wasnt found\n exiting...")
            return False
        except Exception:
            print("error found ! \n ")
            return False
#         to check after clicking next page button the table values are changer or not
        if df.equals(last_df):
            sleep(2)
        else:
            return df


def next_page(driver):
    "this method will check the avilability of next page and move to it "
    print ("moving onto the next page")
    try:
        next_page_button = WebDriverWait(driver, 5).until(
        EC.presence_of_element_located((By.XPATH, './/a[@class="paginate_button next"]')))
        sleep(1)
        next_page_button.click()
        return True
    except TimeoutException:
         try:
            WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.XPATH, './/a[@class="paginate_button next disabled"]')))
            sleep(1)
            print ("reached on last page ")
         except TimeoutException:
            print ("oppss! \ next page button wasnt found\n exiting...")
         return False
    except Exception as e:
        print("error found ! \n ")
        print(e)
        return False

def concate_n_write_dfs(list_of_dfs, output_folder, page_category):
    'this method will concatnate all the dfs and write them into csv'
    appended_df = pd.concat(list_of_dfs, ignore_index=True)
    appended_df.to_csv(f'{output_folder}/{page_category.replace(" ","-")}.csv', index=False)
    print("file saved successfully\n")

base_links={
    'boys' : 'https://laxmath.com/boys/rating001x.php',
    'girls': 'https://laxmath.com/girls/rating001x.php#'
    }

if not os.path.exists(OUTPUT_FOLDER_PATH):
    print(f"{OUTPUT_FOLDER_PATH} not exist")
else:
    driver = webdriver.Chrome(options=opt)
    for key_catgory in base_links.keys():
        print(f"scrapping for {key_catgory}\n")
        driver.get(base_links[key_catgory])
        sleep(5)
        last_df = pd.DataFrame()
        all_dfs_list = []
        while True:
            each_page_df = get_page_table(driver,last_df)
            # Check if the result is a DataFrame or False
            if isinstance(each_page_df, pd.DataFrame):
                all_dfs_list.append(each_page_df)
                last_df = each_page_df
            else:
                break
            if not next_page(driver):
                break
        concate_n_write_dfs(all_dfs_list, OUTPUT_FOLDER_PATH, key_catgory)

    print("completed successfully")
    driver.close()