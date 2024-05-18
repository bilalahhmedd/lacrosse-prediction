""" this py module provides etl to extract and transform camps and clinics data"""

import pandas as pd
import os

from data_helper import get_csv_dataframes_from_folder
from data_helper import set_col_names_lower,set_col_names_underscore_separated
from data_helper import drop_unnamed_columns

from urllib.parse import quote
from sqlalchemy.engine import create_engine

FOLDER_PATH = '../data/campsandclinics'
AIRTABLE_RAW_FOLDER = 'raw/airtable/'
AIRTABLE_RAW_BRONZE_FILE_NAME = 'airtable.csv'
MASS_ELITE_RAW_FOLDER = 'raw/mass_elite/'
MASS_ELITE_BRONZE_FILE_NAME='mass_elite.csv'
OUTPUT_FILE_NAME = 'camps_and_clinics.csv'

WRITE_FLAG=False
WRTIE_DB_FLAG = False

"""
airtable data processing
"""
airtable_csv_dfs = get_csv_dataframes_from_folder(os.path.join(FOLDER_PATH,AIRTABLE_RAW_FOLDER))
airtable_dfs_list = [df for file_name, df in airtable_csv_dfs.items()]

airtable_df=pd.concat(airtable_dfs_list).drop_duplicates()
airtable_df_final=drop_unnamed_columns(set_col_names_underscore_separated(set_col_names_lower(airtable_df)))
airtable_df_final['websource']='airtable'

print(airtable_df_final.head())
if WRITE_FLAG:
    airtable_df_final.to_csv(os.path.join(FOLDER_PATH,'bronze',AIRTABLE_RAW_BRONZE_FILE_NAME))

"""
mass elite data processing
"""
mass_elite_csv_dfs = get_csv_dataframes_from_folder(os.path.join(FOLDER_PATH,MASS_ELITE_RAW_FOLDER))
mass_elite_dfs_list = [df.drop(df.columns[[0]],axis=1) for file_name, df in mass_elite_csv_dfs.items()]
mass_elite_df = pd.concat(mass_elite_dfs_list).drop_duplicates()
mass_elite_df_final=drop_unnamed_columns(set_col_names_underscore_separated(set_col_names_lower(mass_elite_df)))
mass_elite_df_final=mass_elite_df_final.rename(columns={'camp_link':'link'})
mass_elite_df_final['websource']='masselite'

print(mass_elite_df_final.head())

if WRITE_FLAG:
    mass_elite_df_final.to_csv(os.path.join(FOLDER_PATH,'bronze',MASS_ELITE_BRONZE_FILE_NAME))

"""
combine both datasets
"""
camps_clinics_df = pd.concat([airtable_df_final,mass_elite_df_final])
if WRITE_FLAG:
    camps_clinics_df.to_csv(os.path.join(FOLDER_PATH,'bronze',OUTPUT_FILE_NAME))

if WRTIE_DB_FLAG:
    host = '182.75.105.186'
    dev_port = '33096'
    dev_database = 'lacrosse-pre-dev'
    password='L@crosse753'
    user='lacrosse'
    try:
        print('loading camps and clinics data into database ... ')
        engine = create_engine('mysql+mysqlconnector://lacrosse:%s@182.75.105.186:33096/lacrosse-pre-dev' % quote(password))
        camps_clinics_df.to_sql('camps_and_clinics',engine,if_exists='append',index=False)
        print('success')
    except Exception as e:
        print(e)
    