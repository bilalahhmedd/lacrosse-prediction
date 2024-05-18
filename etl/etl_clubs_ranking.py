import os
import pandas as pd
import sys
sys.path.insert(1,os.path.join(os.path.realpath('__file__').split("lacrosse-prediction")[0],'lacrosse-prediction'))

from data_helper import get_csv_dataframes_from_folder
from data_helper import to_camel_case
from data_helper import identify_gender
from etl_helper import create_data_folders
from etl_helper import process_raw_to_bronze, process_bronze_to_silver, process_silver_to_gold

from config.config import DATA_FOLDER_PATH

WRITE_FLAG=True
CATEGORY_NAME = 'clubs_ranking'
RAW_FOLDER_PATH = os.path.join(DATA_FOLDER_PATH,CATEGORY_NAME,'raw')
BRONZE_FOLDER_PATH = os.path.join(DATA_FOLDER_PATH,CATEGORY_NAME,'bronze')
SILVER_FOLDER_PATH = os.path.join(DATA_FOLDER_PATH,CATEGORY_NAME,'silver')
GOLD_FOLDER_PATH = os.path.join(DATA_FOLDER_PATH,CATEGORY_NAME,'gold')
FINAL_STAGE_FOLDER_PATH = os.path.join(DATA_FOLDER_PATH,'final_stage')

SOURCE_FOLDERS = ['insidelacrosse']

create_data_folders(CATEGORY_NAME,DATA_FOLDER_PATH,SOURCE_FOLDERS)

columns_map = {
    
    }

process_raw_to_bronze(RAW_FOLDER_PATH,BRONZE_FOLDER_PATH,column_map_flag=True,columns_map=columns_map)

print('processing bronze folder')

for folder in os.listdir(BRONZE_FOLDER_PATH):
    print(folder)
    bronze_dfs_dict = get_csv_dataframes_from_folder(os.path.join(BRONZE_FOLDER_PATH,folder))
    for file_name, df in bronze_dfs_dict.items():
        print(file_name)
        if 'gender' not in df.columns:
            df['gender']=identify_gender(file_name)+'s'
            print('gender column added')
        clas,season = file_name.split('_')[:2]
        if 'class' not in df.columns:
            df['class']=clas
            print('class column added')
        if 'season' not in df.columns:
            df['season']=season
            print('season column added')
        df.to_csv(os.path.join(BRONZE_FOLDER_PATH,folder,file_name),index=False)
    print('success')

process_bronze_to_silver(BRONZE_FOLDER_PATH,SILVER_FOLDER_PATH)
process_silver_to_gold(SILVER_FOLDER_PATH,GOLD_FOLDER_PATH,f'{CATEGORY_NAME}.csv')

gold_dfs_dict = get_csv_dataframes_from_folder(GOLD_FOLDER_PATH)
dataframe=gold_dfs_dict[f'{CATEGORY_NAME}.csv']
cols_camel_case = [to_camel_case(col) for col in dataframe.columns]
dataframe.columns = cols_camel_case
dataframe['rank']=dataframe['rank'].map(lambda x: None if x=='--' else x)
dataframe['rating']=dataframe['rating'].map(lambda x: None if x=='--' else x)

if WRITE_FLAG:
    dataframe.to_csv(os.path.join(FINAL_STAGE_FOLDER_PATH,f'{CATEGORY_NAME}.csv'),index=False)

