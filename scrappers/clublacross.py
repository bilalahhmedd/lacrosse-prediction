""" this module scrapes data from clublacross.com """
from time import sleep
import os
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
# chromedriver_autoinstaller.install()

SCRAPE_LINK="https://public.clublacrosse.org/commitments"
OUTPUT_FOLDER="../data/clublacrosse"

if not os.path.exists(OUTPUT_FOLDER):
    print('dir created '+str(OUTPUT_FOLDER)+' successfully')
    os.mkdir(OUTPUT_FOLDER)

TEMP_DIR = f'{os.path.abspath(os.getcwd())}/temp'
chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.add_experimental_option("prefs", {
    "download.default_directory": f"{TEMP_DIR}",
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "safebrowsing.enabled": True
})
chrome_options.add_argument("--start-maximized")

def get_csv_files(directory):
    """ method to get all csv files in directory """
    csv_files = [f for f in os.listdir(directory) if f.endswith('.csv')]
    return csv_files
def load_table_recursively(weblink:str,driver,sleep_time=1,thresh_hold=1000):
    """ method to load dynamic table with recursive approach """
    print('waiting to load dynamic table: '+str(sleep_time) + ' seconds')
    driver.get(weblink)
    sleep(sleep_time)
    try:
        driver.find_element(By.XPATH,'.//div[@row-id="row-group-1"]')
        driver.find_element(By.XPATH,'.//div[@row-id="row-group-0"]')
        print('table loaded successfully')
        return True
    except: #Exception("NoSuchElementException")
        if sleep_time > thresh_hold:
            print('load_table_recursively failed with thresholdvalue: '+str(thresh_hold))
            return False
        return load_table_recursively(
            weblink=weblink,
            driver=driver,
            sleep_time=sleep_time+10,
            thresh_hold=thresh_hold
        )
def main():
    """main method to run scrapper"""
    web_driver = webdriver.Chrome(options=chrome_options)
    print('chrome driver loaded successfully')
    print('loading weblink with recursive approach')
    if load_table_recursively(
        weblink=SCRAPE_LINK,
        driver=web_driver,
        sleep_time=15,
        thresh_hold=130
        ):
        categories={
            "clublacrosse_boys": web_driver.find_element(By.XPATH,'.//div[@row-id="row-group-1"]'),
            "clublacrosse_girls": web_driver.find_element(By.XPATH,'.//div[@row-id="row-group-0"]'),
            }
        for name in categories.keys():
            print(f"geting infomation about {name}")
            row=categories[name]
            row.find_element(By.XPATH,'.//div[@col-id="12M"]').click()
            print(
                f"""\ngetting 
                {web_driver.find_element(By.XPATH,'.//h2[@id="customized-dialog-title"]').text}
                \n"""
                )
            sleep(2)
            cel=web_driver.find_elements(By.XPATH,'.//div[@col-id="player_name"]')
            cel=cel[1]
            actions = ActionChains(web_driver)
            # Right-click on the div element
            actions.context_click(cel).perform()
            sleep(1)
            actions.send_keys(Keys.ARROW_UP).send_keys(Keys.ARROW_RIGHT).send_keys(Keys.RETURN).perform()
            sleep(1)
            web_driver.find_element(
                By.XPATH,
                './/button[@class="MuiButtonBase-root MuiIconButton-root MuiIconButton-sizeMedium css-bte7tm"]'
                ).click()
            while True:
                try:
                    csv_files_list = get_csv_files(TEMP_DIR)
                    if csv_files_list:
                        break
                except:
                    pass
            file_name=f"{TEMP_DIR}/export.csv"
            df=pd.read_csv(file_name)
            try:
                os.remove(file_name)
            except Exception as e:
                print(f"An error occurred: {e}")
            print(f"{df.shape} records found")
            df.to_csv(f"{OUTPUT_FOLDER}/{name.replace(' ','_').lower()}.csv", index =False)
            sleep(5)
            print("csv SAVED")
        web_driver.close()
        print("completed...")

if __name__ == "__main__":
    main()
