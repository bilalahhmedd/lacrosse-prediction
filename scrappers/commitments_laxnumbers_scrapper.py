
import pandas as pd
import requests
from bs4 import BeautifulSoup

import os 
import sys
sys.path.insert(1,os.path.join(os.path.realpath('__file__').split("lacrosse-prediction")[0],'lacrosse-prediction'))
from config.config import category_config_dict
configuration = category_config_dict['commitments']
CATEGORY_NAME = configuration['category_name']
WEBSOURCE = configuration['websources_list'][2]
OUTPUT_FOLDER_PATH=os.path.join(configuration['scrapper_output_folder_path'],WEBSOURCE)

HEADERS = {"User-Agent":"Mozilla/5.0 Gecko/20100101 Firefox/33.0 GoogleChrome/10.0"}
BASE_LINK="https://www.laxnumbers.com/recruits.php"

def main():
    if not os.path.exists(OUTPUT_FOLDER_PATH):
        print(f'{OUTPUT_FOLDER_PATH} does not exist. exit')
        return False
    else:
        page = requests.get(BASE_LINK,headers = HEADERS)
        soup = BeautifulSoup(page.content, 'html.parser')
        main_page_table=soup.find("table")
        print('scrapping started for laxnumbers.com')
        for link in main_page_table.find_all("a"):
            
            final_link=(f"https://www.laxnumbers.com/{link['href']}")
            table_name=link.text
            print(f"Getting records from {table_name}\n")
            page = requests.get(final_link,headers = HEADERS)
            soup = BeautifulSoup(page.content, 'lxml')
            tab = soup.find("table",{"class":"show-recruits-table"})
            df = pd.read_html(str(tab))
            df=df[0]
            df.drop(['Fix'], axis = 1, inplace = True)
            print(f"{df.shape} records found")
            
            df.to_csv(f"{OUTPUT_FOLDER_PATH}/{table_name.replace(' ','_').lower()}.csv", index =False)
            
            print("csv SAVED")
            print("\n\n")
    return True

if __name__ == '__main__':
    if main():
        print("success")
    else:
        print("fail")