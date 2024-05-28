import pandas as pd
from time import sleep
from selenium.webdriver.common.keys import Keys
from selenium import webdriver
import os 
import sys
sys.path.insert(1,os.path.join(os.path.realpath('__file__').split("lacrosse-prediction")[0],'lacrosse-prediction'))
import chromedriver_autoinstaller
chromedriver_autoinstaller.install()
from selenium.webdriver.common.action_chains import ActionChains

from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support.ui import Select


from config.config import category_config_dict
configuration = category_config_dict['commitments']
CATEGORY_NAME = configuration['category_name']
WEBSOURCE = configuration['websources_list'][4]
OUTPUT_FOLDER_PATH=os.path.join(configuration['scrapper_output_folder_path'],WEBSOURCE)

WRITE_FLAG = True

opt = webdriver.ChromeOptions()
opt.add_argument("--start-maximized")
opt.add_experimental_option("excludeSwitches", ["disable-popup-blocking"])



scrape_links = {
    "girls_commitments": "https://www.insidelacrosse.com/recruiting/allcommitments/girls",               
    "boys_commitments": "https://www.insidelacrosse.com/recruiting/allcommitments"
    }

def load_dynamic_main_table(
    web_driver,
    url,
    range_input=1000,
    sleep_interval=1
    ):
    """loads dynamic table page with scrolling """
    web_driver.get(url)
    sleep(5)
    print("scrolling to laod complete main table")
    previous_length = 0
    counter =0
    for x in range (range_input):
        print(f"loop: {x}")
        web_driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        sleep(sleep_interval)
        results=web_driver.find_elements(by=By.XPATH, value='.//table[@class="table box table-striped"]')
        if previous_length <= len(results):
            counter += 1
        else:
            counter = 0
        previous_length = len(results)
        table_html = results[-1].get_attribute("outerHTML")
        # check if table is empty
        df = pd.read_html(table_html)[0]
        if df.shape[0] == 0:
            break
        if counter > 20:
            print("table loading finished")
            break
    return True

def fetch_main_table_from_loaded_page(web_driver:object):
    """fetchs table data from loaded page """
    final_dfs=[]
    print("retrieving all data.... \n")

    #     get all table's data and concatenate them
    results=web_driver.find_elements(by=By.XPATH, value='.//table[@class="table box table-striped"]')
    for each_table in results:
        # Use Pandas to read HTML and convert it to a DataFrame
        df = pd.read_html(each_table.get_attribute("outerHTML"))[0]
        links = []
        for row_index, row in df.iterrows():
            link_elements = each_table.find_elements(by=By.XPATH, value=f'.//tr[{row_index + 1}]/td[1]//a')
            # Extract the href attribute from the first link element, if any
            link = link_elements[0].get_attribute("href") if link_elements else None
            links.append(link)
        # Add a new column to the DataFrame with the extracted links
        df["Link"] = links
        final_dfs.append(df)
    result_df=pd.concat(final_dfs, axis=0, ignore_index=True)
    return result_df


def main():
    if not os.path.exists(OUTPUT_FOLDER_PATH):
        print(f"{OUTPUT_FOLDER_PATH} not exist")
        return False
    else:    
        driver = webdriver.Chrome(options=opt)

        # girls_commitment data
        print('downloading girls_commitments')
        load_dynamic_main_table(driver,scrape_links['girls_commitments'],sleep_interval=3)
        scrapped_df=fetch_main_table_from_loaded_page(driver)
        if WRITE_FLAG:
            scrapped_df.to_csv(os.path.join(OUTPUT_FOLDER_PATH,'girls_commitments.csv'))

        # boys commitment data
        print('downloading boys_commitments')
        load_dynamic_main_table(driver,scrape_links['boys_commitments'],sleep_interval=3)
        scrapped_df=fetch_main_table_from_loaded_page(driver)
        if WRITE_FLAG:
            scrapped_df.to_csv(os.path.join(OUTPUT_FOLDER_PATH,'boys_commitments.csv'))
        return True

if __name__ == "__main__":
    if main():
        print("success")
    else:
        print("fail")