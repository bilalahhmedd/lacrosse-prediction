import os
from time import sleep
from datetime import datetime
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
# from selenium.webdriver.support import expected_conditions as EC
# from selenium.webdriver.support.wait import WebDriverWait

# import chromedriver_autoinstaller
# chromedriver_autoinstaller.install()

opt = webdriver.ChromeOptions()
opt.add_argument("--start-maximized")
opt.add_experimental_option("excludeSwitches", ["disable-popup-blocking"])
# opt.add_argument("--headless")
# opt.add_argument("--lang=en")

WRITE_FLAG=True

root_data_folder="../data/raw/campsandclinincs/"
output_folder_name="airtable"
output_file_name=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
output_file_path = os.path.join(root_data_folder,output_folder_name)
if not os.path.exists(output_file_path):
    os.makedirs(output_file_path)
    print(f"{output_file_path} folder created success")


base_link="https://airtable.com/app4Y6v4bZxmLOcek/shrLGPI1xHHYhLflR/tblkRKZ9XVnWNQjI3?backgroundColor=green&layout=card&viewControls=on"

def load_webpage_dynamically(
        weblink:str,
        driver_obj:object,
        sleep_time=2,
        increment=1,
        limit=10
    ):
    """to load webpage recursively"""
    print(f"getting {weblink}")
    print(f'sleep time: {sleep_time} increment: {increment}')
    driver_obj.get(weblink)
    if limit < sleep_time+increment:
        return False
    sleep(sleep_time)
    # check if page is loaded
    results = driver_obj.find_elements(by=By.XPATH,value='.//div[@class="draggableRecord animate"]')
    if len(results)==0:
        return load_webpage_dynamically(
            weblink=weblink,
            driver_obj=driver_obj,
            sleep_time=sleep_time+increment,
            increment=increment+1,
            limit=limit
            )
    return True

def fetch_webpage_table_data(
        driver_obj:object
):
    """ fetch table data from weblink"""
    all_ids=[]
    all_rows_dict=[]
    continue_scrolling=True
    while continue_scrolling:
        try:
            print("scrapping all rows please wait....")
            init_length=len(all_ids)
            results=driver_obj.find_elements(by=By.XPATH, value='.//div[@class="draggableRecord animate"]')
            #     get all the results present in DOM
            for rzlt in results:
            #         check if row id is already scrapped or not
                if rzlt.get_attribute("data-rowid") not in all_ids:
                    each_row_dict={}
                    rzlt.find_element(
                        by=By.XPATH,
                        value='.//div[@class="col-12 mb1 truncate strong text-size-large line-height-3"]'
                        ).text
                    all_details=rzlt.find_elements(
                        by=By.XPATH,
                        value='.//div[@class="flex-none overflow-hidden pr1"]'
                        )

            #  get all the entries in each row
                    for entry in all_details:
                        each_feild=entry.text.split("\n")
                        if len(each_feild)==2:
                            each_row_dict[each_feild[0]]=each_feild[-1]
            # if row is empty then
                        if len(each_feild)==1:
                            each_row_dict[each_feild[0]]=""
                    all_ids.append(rzlt.get_attribute("data-rowid"))
                    all_rows_dict.append(each_row_dict)
            #     scroll to load more results into DOM
            driver_obj.execute_script("arguments[0].scrollIntoView(true);", results[-1])
            sleep(2)
            # check if new rows are loaded into dom or its end of page
            if init_length == len(all_ids):
                continue_scrolling=False
        except Exception as e:
            print("exception caught while scraping data...")
            print(e)
            continue_scrolling=False

    return all_rows_dict

def main():
    """main execution of code """
    driver = webdriver.Chrome(options=opt)
    if load_webpage_dynamically(base_link,driver):
        all_rows_dict = fetch_webpage_table_data(driver)
        if WRITE_FLAG:
            df_main = pd.DataFrame(all_rows_dict)
            if df_main.shape != (0,0):
                df_main.to_csv(f"{output_file_path}/{output_file_name}.csv", index=False)
                print(f'completed,\n{df_main.shape} record found, \n {output_file_name} File saved successfully...')
            else:
                print('data frame is empty')
    else:
        print('main page loading failed')


if __name__ =="__main__":
    main()