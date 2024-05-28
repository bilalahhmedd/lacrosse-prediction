import pandas as pd
from time import sleep
from selenium import webdriver
import os
import sys
sys.path.insert(1,os.path.join(os.path.realpath('__file__').split("lacrosse-prediction")[0],'lacrosse-prediction'))
import chromedriver_autoinstaller
chromedriver_autoinstaller.install()
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from config.config import category_config_dict

configuration = category_config_dict['clubs_ranking']
CATEGORY_NAME = configuration['category_name']
WEBSOURCE = configuration['websources_list'][0]
OUTPUT_FOLDER_PATH=os.path.join(configuration['scrapper_output_folder_path'],WEBSOURCE)
opt = webdriver.ChromeOptions()
opt.add_argument("--start-maximized")
opt.add_experimental_option("excludeSwitches", ["disable-popup-blocking"])
opt.add_argument('--disable-dev-shm-usage')

CREATE_CONFIG_FLAG = False
SAVE_DATA_FLAG = True
UPDATE_CONFIG_FLAG=True
# value in config file status column to trigger scrapper
STATUS_CONFIG_FOLDER_PATH = configuration['status_config_folder_path']
STATUS_VALUE_TO_SCRAPE=['No','Fail']
STATUS_CSV_FILE='insidelacrosse_url_scraping_status.csv'

def scroll_down_page(driver, speed=8,sleep_interval=1):
    current_scroll_position, new_height= 0, 1
    while current_scroll_position <= new_height:
        current_scroll_position += speed
        driver.execute_script("window.scrollTo(0, {});".format(current_scroll_position))
        new_height = driver.execute_script("return document.body.scrollHeight")
        sleep(1)

def scroll_down_slowly(driver, speed=3000, sleep_interval=3,scrolling_steps=20):
    y = speed
    for timer in range(0,scrolling_steps):
        driver.execute_script("window.scrollTo(0, "+str(y)+")")
        y += speed  
        print(f'timer: {timer}')
        sleep(sleep_interval)
        

def load_dynamic_main_table(
    web_driver,
    url,
    sleep_interval,
    speed=3000,
    scroll_sleep_interval=3,
    scrolling_steps_count=20    
    ):
    """load main table using dynamic approach"""
    web_driver.get(url)
    sleep(sleep_interval)
    print("scrolling to laod complete main table")
    scroll_down_slowly(web_driver,speed,scroll_sleep_interval,scrolling_steps_count)
    return True

def fetch_main_table_from_loaded_page(driver):
    final_dfs=[]
    print("retrieving all data.... \n")

    #     get all table's data and concatenate them
    results=driver.find_elements(by=By.XPATH, value='.//table[@class="table box table-striped"]')
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


if not os.path.exists(OUTPUT_FOLDER_PATH):
    print(f"{OUTPUT_FOLDER_PATH} not exist ")
else:

    club_ranking_main_url = "https://www.insidelacrosse.com/recruiting/club"
    gender_list=["girls","boys"]
    class_list = [x for x in range(2024,2029)]
    session_list = [x for x in range(2019,2025)]

    # create config file to track status and resume scraping
    if CREATE_CONFIG_FLAG:

        urls = []
        status=[]
        for gender in gender_list:
            for clas in class_list:
                for session in session_list:
                    url_to_scrape = club_ranking_main_url+f'/{clas}/{session}/{gender}'
                    urls.append(url_to_scrape)
                    status.append('No')
        scraping_status_df=pd.DataFrame({'url':urls,'status':status})
        scraping_status_df.to_csv(os.path.join(STATUS_CONFIG_FOLDER_PATH,STATUS_CSV_FILE),index=False)
        scraping_status_df.head()

    # read scraping status csv file
    scraping_status_df = pd.read_csv(os.path.join(STATUS_CONFIG_FOLDER_PATH,STATUS_CSV_FILE))

    # initiate webdriver
    driver = webdriver.Chrome(options=opt)

    # iterate over dataframe
    for index, row in scraping_status_df.iterrows():
        if row['status'] in STATUS_VALUE_TO_SCRAPE:
            # start scraping data
            try:
                url_to_scrape = row['url']
                print(url_to_scrape)
                load_dynamic_main_table(driver,row['url'],sleep_interval=5,speed=2000,scroll_sleep_interval=3)
                output_df=fetch_main_table_from_loaded_page(driver)
                file_to_save = url_to_scrape.split('club/')[-1].replace('/','_')+'.csv'
                print(file_to_save)
                if SAVE_DATA_FLAG:
                    output_df.to_csv(os.path.join(OUTPUT_FOLDER_PATH,file_to_save),index=False)
                print('saved success')
                scraping_status_df.loc[index,'status']='Yes'
                del(output_df)
            except Exception as e:
                print(e)
                print('data save failed')
                scraping_status_df.loc[index,'status']='Fail'
                # update status col with Failed
                # scraping_status_df
            if index > 2 and index%2 ==0:
                if UPDATE_CONFIG_FLAG:
                    scraping_status_df.to_csv(os.path.join(STATUS_CONFIG_FOLDER_PATH,STATUS_CSV_FILE),index=False)
                print(f'config file updated')
        else:
            print(f"skipping url: {row['url']} with status: {row['status']}")
