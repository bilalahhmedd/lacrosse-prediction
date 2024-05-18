""" this notebook provides etl to extract and transform club ranking data"""

import os
import sys
sys.path.insert(1,os.path.join(os.path.realpath('__file__').split("lacrosse-prediction")[0],'lacrosse-prediction'))

import pandas as pd
from data_helper import get_csv_dataframes_from_folder
from data_helper import map_col_names
from data_helper import identify_gender
from etl_helper import create_data_folders
from etl_helper import process_raw_to_bronze, process_bronze_to_silver, process_silver_to_gold

from config.config import DATA_FOLDER_PATH
from config.config import category_config_dict

configuration = category_config_dict['schools_ranking']
CATEGORY_NAME = configuration['category_name']

WRITE_FLAG=True
RAW_FOLDER_PATH = os.path.join(DATA_FOLDER_PATH,CATEGORY_NAME,'raw')
BRONZE_FOLDER_PATH = os.path.join(DATA_FOLDER_PATH,CATEGORY_NAME,'bronze')
SILVER_FOLDER_PATH = os.path.join(DATA_FOLDER_PATH,CATEGORY_NAME,'silver')
GOLD_FOLDER_PATH = os.path.join(DATA_FOLDER_PATH,CATEGORY_NAME,'gold')
FINAL_STAGE_FOLDER_PATH = os.path.join(DATA_FOLDER_PATH,'final_stage')

SOURCE_FOLDERS = ['laxmath','insidelacrosse']

create_data_folders(CATEGORY_NAME,DATA_FOLDER_PATH,SOURCE_FOLDERS)
columns_map = {
    'champ_%_rank':'rank',
    'pr':'powerRating',
    'sos-pr':'strengthOfSchedulePR',
    'qwf-pr':'qualityWinFactorPR',
    'champ_%':'championshipPercentage',
    'rpi':'ratingPercentageIndex',
    'sos-rpi':'strengthOfScheduleRPI',
    'qwf-rpi':'qualityWinFactorRPI',
    'selection':'selection',
    'champion_%':'championPercentage',
    'web_source':'webSource',
    }

process_raw_to_bronze(RAW_FOLDER_PATH,BRONZE_FOLDER_PATH)
print('processing bronze folder')
for folder in os.listdir(BRONZE_FOLDER_PATH):
    print(folder)
    bronze_dfs_dict = get_csv_dataframes_from_folder(os.path.join(BRONZE_FOLDER_PATH,folder))
    for file_name, df in bronze_dfs_dict.items():
        print(file_name)
        if 'gender' not in df.columns:
            df['gender']=identify_gender(file_name)+'s'
            print('gender column added')
        df.to_csv(os.path.join(BRONZE_FOLDER_PATH,folder,file_name),index=False)
    print('success')

process_bronze_to_silver(BRONZE_FOLDER_PATH,SILVER_FOLDER_PATH)
process_silver_to_gold(SILVER_FOLDER_PATH,GOLD_FOLDER_PATH,f'{CATEGORY_NAME}.csv')

gold_dfs_dict = get_csv_dataframes_from_folder(SILVER_FOLDER_PATH)
dataframe=gold_dfs_dict['laxmath.csv']
dataframe['winRatio']= dataframe['team'].map(lambda x: x.split('( ')[1].split(' )')[0] if (type(x)==str and '(' in x) else None)
dataframe['team']=dataframe['team'].map(lambda x: x.split('( ')[0] if type(x)==str else x)
dataframe['champ_%']=dataframe['champ_%'].map(lambda y: float(y.split('%')[0].replace(' ','')) if type(y)==str else y)
dataframe=map_col_names(dataframe,columns_map)
if WRITE_FLAG:
    dataframe.to_csv(os.path.join(FINAL_STAGE_FOLDER_PATH,f'{CATEGORY_NAME}.csv'),index=False)
