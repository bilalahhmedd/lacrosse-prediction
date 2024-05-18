""" this module provides etl to extract and transform camps and clinics data """

import os
import pandas as pd
import sys
sys.path.insert(1,os.path.join(os.path.realpath('__file__').split("lacrosse-prediction")[0],'lacrosse-prediction'))


from data_helper import get_csv_dataframes_from_folder
from data_helper import to_camel_case
from etl_helper import create_data_folders
from etl_helper import process_raw_to_bronze, process_bronze_to_silver, process_silver_to_gold
from data_helper import set_date_format

from config.config import DATA_FOLDER_PATH
WRITE_FLAG=True

CATEGORY_NAME = 'camps_and_clinics'
RAW_FOLDER_PATH = os.path.join(DATA_FOLDER_PATH,CATEGORY_NAME,'raw')
BRONZE_FOLDER_PATH = os.path.join(DATA_FOLDER_PATH,CATEGORY_NAME,'bronze')
SILVER_FOLDER_PATH = os.path.join(DATA_FOLDER_PATH,CATEGORY_NAME,'silver')
GOLD_FOLDER_PATH = os.path.join(DATA_FOLDER_PATH,CATEGORY_NAME,'gold')
FINAL_STAGE_FOLDER_PATH = os.path.join(DATA_FOLDER_PATH,'final_stage')
SOURCE_FOLDERS = ['airtable','mass_elite']

def process_date_columns_camps_clinics(
        input_df:object,
        ):
    """ process date column for given dataframe"""
    input_df['eventDate']=pd.to_datetime(input_df['eventDate']).map(set_date_format)
    input_df['added']=pd.to_datetime(input_df['added']).map(lambda x : set_date_format(x) if type(x)==pd.Timestamp else None)
    return input_df



create_data_folders(CATEGORY_NAME,DATA_FOLDER_PATH,SOURCE_FOLDERS)

columns_map = {
    'Added/Edited':'added',
    'CAMP LINK':'link',
    'SUMMER CAMPS/CLINICS':'event',
    'Description':'event',
    'DATE':'event_date',
    'LOCATION':'state',
    'FOR':'year',
    'Type':'gender',
    }

process_raw_to_bronze(RAW_FOLDER_PATH,BRONZE_FOLDER_PATH,column_map_flag=True,columns_map=columns_map)
process_bronze_to_silver(BRONZE_FOLDER_PATH,SILVER_FOLDER_PATH)
process_silver_to_gold(SILVER_FOLDER_PATH,GOLD_FOLDER_PATH,'camps_and_clinics.csv')

gold_dfs_dict = get_csv_dataframes_from_folder(GOLD_FOLDER_PATH)
dataframe=gold_dfs_dict['camps_and_clinics.csv']
cols_camel_case = [to_camel_case(col) for col in dataframe.columns]
dataframe.columns = cols_camel_case
dataframe = process_date_columns_camps_clinics(dataframe)

if WRITE_FLAG:
    dataframe.to_csv(os.path.join(FINAL_STAGE_FOLDER_PATH,f'{CATEGORY_NAME}.csv'),index=False)