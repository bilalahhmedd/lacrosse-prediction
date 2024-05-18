""" college ranking laxmax scraping module """

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


def get_page_table(last_df):
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
        except TimeoutException as ex:
            print ("oppss!\ntable wasnt found\n exiting...")
            return False
        except Exception as e:
            print("error found ! \n ")
            return False
#         to check after clicking next page button the table values are changer or not
        if df.equals(last_df):
            sleep(2)
        else:
            return df

def next_page():
    "this method will check the avilability of next page and move to it "
    print ("moving onto the next page")
    try:
        next_page_button = WebDriverWait(driver, 5).until(
        EC.presence_of_element_located((By.XPATH, './/a[@class="paginate_button next"]')))
        sleep(1)
        next_page_button.click()
        return True
    except TimeoutException as ex:
         try:
            WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.XPATH, './/a[@class="paginate_button next disabled"]')))
            sleep(1)
            print ("reached on last page ")
         except TimeoutException as ex:
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
    "LAX MATH COLLEGE DIVISION 1 (MEN)": r"https://laxmath.com/men/rating001x.php",
    'LAX MATH COLLEGE DIVISION 2 (MEN)': 'https://laxmath.com/men/rating002x.php',
    'LAX MATH COLLEGE DIVISION 3 (MEN)': 'https://laxmath.com/men/rating003x.php',
    'LAX MATH COLLEGE JUCO (MEN)': 'https://laxmath.com/men/rating004x.php',
    'LAX MATH COLLEGE NAIA (MEN)': 'https://laxmath.com/men/rating007x.php',
    'LAX MATH COLLEGE CLUB GLL (MEN)': 'https://laxmath.com/glll/rating010x.php',
    'LAX MATH COLLEGE CLUB NCLL DIVISION 1  (MEN)': 'https://laxmath.com/ncll/rating008x.php',
    'LAX MATH COLLEGE CLUB NCLL DIVISION 2 (MEN)': 'https://laxmath.com/ncll/rating009x.php',
    'LAX MATH COLLEGE MCLA DIVISION 1 (MEN)': 'https://laxmath.com/mcla/rating001x.php',
    'LAX MATH COLLEGE MCLA DIVISION 2 (MEN)': 'https://laxmath.com/mcla/rating002x.php',
    'LAX MATH COLLEGE MCLA DIVISION 3 (MEN)': 'https://laxmath.com/mcla/rating003x.php',
    'LAX MATH COLLEGE DIVISION 1 (WOMEN)': 'https://laxmath.com/wom/rating001x.php',
    'LAX MATH COLLEGE DIVISION 2 (WOMEN)': 'https://laxmath.com/wom/rating002x.php',
    'LAX MATH COLLEGE DIVISION 3 (WOMEN)': 'https://laxmath.com/wom/rating003x.php',
    'LAX MATH COLLEGE NAIA (WOMEN)': 'https://laxmath.com/wom/rating007x.php'}

if __name__ == "__main__":
    output_folder_name="../data/extra/college_ranking/raw/laxmath/"
    if (not os.path.exists(output_folder_name)):
        os.mkdir(output_folder_name)
    driver = webdriver.Chrome(options=opt)

    for key_catgory in base_links.keys():
        print(f"scrapping for {key_catgory}\n")
        driver.get(base_links[key_catgory])
        sleep(5)
        last_df = pd.DataFrame()
        all_dfs_list = []
        while True:
            each_page_df = get_page_table(last_df)
            # Check if the result is a DataFrame or False
            if isinstance(each_page_df, pd.DataFrame):
                all_dfs_list.append(each_page_df)
                last_df = each_page_df
            else:
                break
            if not next_page():
                break
        concate_n_write_dfs(all_dfs_list, output_folder_name, key_catgory)
    
    print("completed successfully")
    driver.close()
