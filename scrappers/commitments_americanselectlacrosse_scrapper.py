import os 
import sys
sys.path.insert(1,os.path.join(os.path.realpath('__file__').split("lacrosse-prediction")[0],'lacrosse-prediction'))

import requests
import pandas as pd
from bs4 import BeautifulSoup

from config.config import category_config_dict

configuration = category_config_dict['commitments']
CATEGORY_NAME = configuration['category_name']
WEBSOURCE = configuration['websources_list'][0]
OUTPUT_FOLDER_PATH=os.path.join(configuration['scrapper_output_folder_path'],WEBSOURCE)

HEADERS = {"User-Agent":"Mozilla/5.0 Gecko/20100101 Firefox/33.0 GoogleChrome/10.0"}
LINKS_TO_SCRAPE = {
    "2025-collegiate-commitments":"https://americanselectlacrosse.com/2025-collegiate-commitments",
    "2024-collegiate-commitments":"https://americanselectlacrosse.com/2024-collegiate-commitments",
    "commitments":"https://americanselectlacrosse.com/commitments",
    }

def main():
    print('running scrapper for americanselectlacrosse.com')
    if (not os.path.exists(OUTPUT_FOLDER_PATH)):
        print(f"{OUTPUT_FOLDER_PATH} does not exist. exit")
        return False
    else:
        for key in LINKS_TO_SCRAPE.keys():
            print(f"scrapping for {key}")
            page = requests.get(LINKS_TO_SCRAPE[key],headers = HEADERS)
            soup = BeautifulSoup(page.content, 'html.parser')
            main_page_table=soup.find_all("table")
            df = pd.read_html(str(main_page_table))
            df=df[0]
            print(f"{df.shape} records found")
            df.to_csv(f"{OUTPUT_FOLDER_PATH}/{key.replace(' ','_').lower()}.csv", index =False)
            print("csv SAVED")
            
        print("completed")
    return True

if __name__ == "__main__":
    if main():
        print('success')
    else:
        print('fail')

