""" this module contains scrapping bot to scrap data from toplaxrecruits.com """
import os
from time import sleep
import pandas as pd
from selenium.webdriver.common.keys import Keys
from selenium import webdriver
import chromedriver_autoinstaller
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.chrome.options import Options

from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support.ui import Select
chrome_options = Options()
chrome_options.add_argument("--start-maximized")
# chrome_options.add_argument("--headless")
# chromedriver_autoinstaller.install()

SCRAPE_LINK="https://toplaxrecruits.com/"
OUTPUT_FOLDER="../data/toplaxrecruits"
if not os.path.exists(OUTPUT_FOLDER):
    os.mkdir(OUTPUT_FOLDER)


def main():
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
    # cat="2024 Boys"
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
            except:
                pass

            dfs_list.append(df)
            try:
                driver.find_element(By.XPATH,'.//a[@class="paginate_button next"]').click()

            except:
                break


        result_df=pd.concat(dfs_list, axis=0, ignore_index=True)
        result_df
        print(f"{result_df.shape} records found\n")

        result_df.to_csv(f"{OUTPUT_FOLDER}/{cat.replace(' ','_').lower()}.csv", index =False)

        print("csv SAVED")

if __name__ == "__main__":
    main()
