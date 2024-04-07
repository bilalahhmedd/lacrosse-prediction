""" this py module provides etl to extract and transform camps and clinics data"""

import pandas as pd
import os

from data_helper import get_csv_dataframes_from_folder
from data_helper import set_col_names_lower,set_col_names_underscore_separated
from data_helper import drop_unnamed_columns


FOLDER_PATH = '../data/campsandclinics'
AIRTABLE_RAW_FOLDER = 'raw/airtable/'
AIRTABLE_RAW_BRONZE_FILE_NAME = 'airtable.csv'
MASS_ELITE_FOLDER = '/raw/mass_elite/'
MASS_ELITE_BRONZE_FILE_NAME='mass_elite.csv'
OUTPUT_FILE_NAME = 'camps_and_clinics.csv'

WRITE_FLAG=True
"""
airtable data processing
"""
airtable_csv_dfs = get_csv_dataframes_from_folder(os.path.join(FOLDER_PATH,AIRTABLE_RAW_FOLDER))
airtable_dfs_list = [df for file_name, df in airtable_csv_dfs.items()]

airtable_df=pd.concat(airtable_dfs_list).drop_duplicates()
airtable_df_final=drop_unnamed_columns(set_col_names_underscore_separated(set_col_names_lower(airtable_df)))
# print(airtable_df_final.head())
if WRITE_FLAG:
    airtable_df_final.to_csv(os.path.join(FOLDER_PATH,'/bronze/',AIRTABLE_RAW_BRONZE_FILE_NAME))

"""
mass elite data processing
"""
mass_elite_csv_dfs = get_csv_dataframes_from_folder(os.path.join(FOLDER_PATH,MASS_ELITE_FOLDER))
mass_elite_dfs_list = [df.drop(df.columns[[0]],axis=1) for file_name, df in mass_elite_csv_dfs.items()]
mass_elite_df = pd.concat(mass_elite_dfs_list).drop_duplicates()
mass_elite_df_final=drop_unnamed_columns(set_col_names_underscore_separated(set_col_names_lower(mass_elite_df)))
# print(mass_elite_df_final.head())

if WRITE_FLAG:
    airtable_df_final.to_csv(os.path.join(FOLDER_PATH,'/bronze/',MASS_ELITE_BRONZE_FILE_NAME))

"""
combine both datasets
"""
camps_clinics_df = pd.concat([airtable_df_final,mass_elite_df_final])

