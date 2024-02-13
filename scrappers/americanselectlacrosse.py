import requests
import os 
import pandas as pd
from bs4 import BeautifulSoup
header = {"User-Agent":"Mozilla/5.0 Gecko/20100101 Firefox/33.0 GoogleChrome/10.0"}

# output_folder_name="americanselectlacrosse"
output_folder_name="../data/americanselectlacrosse/"
if (not os.path.exists(output_folder_name)):
    os.mkdir(output_folder_name)

links_to_scrape={
    "2025-collegiate-commitments":"https://americanselectlacrosse.com/2025-collegiate-commitments",
    "2024-collegiate-commitments":"https://americanselectlacrosse.com/2024-collegiate-commitments",
    "commitments":"https://americanselectlacrosse.com/commitments"
    }

for key in links_to_scrape.keys():
    print(f"scrapping for {key}")
    page = requests.get(links_to_scrape[key],headers = header)
    soup = BeautifulSoup(page.content, 'html.parser')
    # soup = BeautifulSoup(page.content, 'lxml')
    main_page_table=soup.find_all("table")
    df = pd.read_html(str(main_page_table))
    df=df[0]
    print(f"{df.shape} records found")
    df.to_csv(f"{output_folder_name}/{key.replace(' ','_').lower()}.csv", index =False)
    print("csv SAVED")
    
print("completed")

