import requests
import os 
import pandas as pd
from bs4 import BeautifulSoup
header = {"User-Agent":"Mozilla/5.0 Gecko/20100101 Firefox/33.0 GoogleChrome/10.0"}

base_link="https://www.laxnumbers.com/recruits.php"
output_folder_name="../data/laxnumbers"
if (not os.path.exists(output_folder_name)):
    os.mkdir(output_folder_name)


page = requests.get(base_link,headers = header)
soup = BeautifulSoup(page.content, 'html.parser')

main_page_table=soup.find("table")

for link in main_page_table.find_all("a"):
    
    final_link=(f"https://www.laxnumbers.com/{link['href']}")
    table_name=link.text
    print(f"Getting records from {table_name}\n")
    page = requests.get(final_link,headers = header)
    soup = BeautifulSoup(page.content, 'lxml')
    tab = soup.find("table",{"class":"show-recruits-table"})
    df = pd.read_html(str(tab))
    df=df[0]
    df.drop(['Fix'], axis = 1, inplace = True)
    print(f"{df.shape} records found")
    
    df.to_csv(f"{output_folder_name}/{table_name.replace(' ','_').lower()}.csv", index =False)
    
    print("csv SAVED")
    print("\n\n")