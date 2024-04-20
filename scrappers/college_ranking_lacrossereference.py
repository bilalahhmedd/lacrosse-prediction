""" college reference scraping module """

import os
from time import sleep
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
from selenium.common.exceptions import TimeoutException
opt = webdriver.ChromeOptions()
opt.add_argument("--start-maximized")
opt.add_experimental_option("excludeSwitches", ["disable-popup-blocking"])

def get_page_table(driver):
    "this method will get all the entries on the page "
    print("scrolling and getting all the rows")
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    sleep(2)
    try:
        table_div = WebDriverWait(driver, 60).until(
                    EC.presence_of_element_located((By.XPATH, './/div[@id="js_div"]'))
                )
        sleep(3)
        header_div=table_div.find_element(by=By.XPATH, value=f'.//div[@class="bbottom col-12 no-padding flex"]')
        # get col names
        col_names=header_div.text.strip().split("\n")
        col_names.insert(0, "index")
        # get all the rows
        rows_divs=table_div.find_elements(by=By.XPATH, value=f'.//div[@class="table-row col-12 no-padding flex"]')

        all_rows_entries_list=[]

        for each_row in rows_divs:
        #     get all cols of each row 
            each_row_entries=[]
            index_team=each_row.find_element(by=By.XPATH, value=f'.//div[@class="col-2-4 left no-padding flex"]').text
            each_row_entries=index_team.split("\n")
            right_cols=each_row.find_elements(by=By.XPATH, value=f'.//div[@class="col-10-8 no-padding"]/ul/li')
            for each_col in right_cols:
                each_row_entries.append(each_col.text)
            all_rows_entries_list.append(each_row_entries)
        
        
        return col_names, all_rows_entries_list
    except TimeoutException as ex:
        print ("oppss!\ntable wasnt found\n exiting...")
        return False
    except Exception as e:
        print("error found ! \n ")
        return False


def concate_n_write_df(col_names, rows_list, output_folder, page_category):
    'this method will concatnate all the rows and write df  them into csv'
    df = pd.DataFrame(rows_list, columns=col_names)
    df.to_csv(f'{output_folder}/{page_category.replace(" ","-")}.csv', index=False)
    print("file saved successfully\n")



base_links={'LAX REFERENCE COLLEGE DIVISION 1 (WOMEN)' : 'https://lacrossereference.com/stats/rpi-d1-women/',
            'LAX REFERENCE COLLEGE DIVISION 2 (WOMEN)': 'https://lacrossereference.com/stats/rpi-d2-women/',
            'LAX REFERENCE COLLEGE DIVISION 3 (WOMEN)': 'https://lacrossereference.com/stats/rpi-d3-women/',
            'LAX REFERENCE COLLEGE DIVISION 1 (MEN)': 'https://lacrossereference.com/stats/rpi-d1-men/',
            'LAX REFERENCE COLLEGE DIVISION 2 (MEN)': 'https://lacrossereference.com/stats/rpi-d2-men/',
            'LAX REFERENCE COLLEGE DIVISION 3 (MEN)': 'https://lacrossereference.com/stats/rpi-d3-men/'
}

output_folder_name="../data/extra/college_ranking/raw/lacrosse_reference"



if __name__ == "__main__":
    if (not os.path.exists(output_folder_name)):
        os.mkdir(output_folder_name)
        print(f'{output_folder_name} created success')
    driver = webdriver.Chrome(options=opt)
    for key_catgory in base_links.keys():
        print(f"scrapping for {key_catgory}\n")
        driver.get(base_links[key_catgory])
        col_names, rows_list = get_page_table(driver)
        concate_n_write_df(col_names, rows_list, output_folder_name, key_catgory)
    
    print("completed successfully")
    driver.close()