import requests
import os 
import pandas as pd
from bs4 import BeautifulSoup

HEADERS = {"User-Agent":"Mozilla/5.0 Gecko/20100101 Firefox/33.0 GoogleChrome/10.0"}
OUTPUT_FOLDER = "../data/americanselectlacrosse/"
LINKS_TO_SCRAPE = {
    "2025-collegiate-commitments":"https://americanselectlacrosse.com/2025-collegiate-commitments",
    "2024-collegiate-commitments":"https://americanselectlacrosse.com/2024-collegiate-commitments",
    "commitments":"https://americanselectlacrosse.com/commitments",
    }

def main():
    print('running scrapper for americanselectlacrosse.com')
    if (not os.path.exists(OUTPUT_FOLDER)):
        os.mkdir(OUTPUT_FOLDER)
    for key in LINKS_TO_SCRAPE.keys():
        print(f"scrapping for {key}")
        page = requests.get(LINKS_TO_SCRAPE[key],headers = HEADERS)
        soup = BeautifulSoup(page.content, 'html.parser')
        # soup = BeautifulSoup(page.content, 'lxml')
        main_page_table=soup.find_all("table")
        df = pd.read_html(str(main_page_table))
        df=df[0]
        print(f"{df.shape} records found")
        df.to_csv(f"{OUTPUT_FOLDER}/{key.replace(' ','_').lower()}.csv", index =False)
        print("csv SAVED")
        
    print("completed")

if __name__ == "__main__":
    main()

