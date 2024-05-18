""" this module contains etl for insidelacrosse club ranking data """

import pandas as pd
import os
import sys
from data_helper import get_csv_dataframes_from_folder
from data_helper import set_col_names_lower,set_col_names_underscore_separated
from data_helper import drop_unnamed_columns

from urllib.parse import quote
from sqlalchemy.engine import create_engine

WRITE_LOCAL_FLAG=True
WRITE_DB_FLAG=True

# extract
raw_folder_path = '../data/extra/clubranking/insidelacrosse/output/raw/'
bronze_folder_path = '../data/extra/clubranking/insidelacrosse/output/bronze/'
bronze_file_name = 'clubsranking.csv'
dataframe_dict=get_csv_dataframes_from_folder(raw_folder_path)

# transform
dfs_list = []
for file_name, dataframe in dataframe_dict.items():
    clas,season,gender=file_name.split('.csv')[0].split('_')
    dataframe['class']=clas
    dataframe['season']=season
    dataframe['gender']=gender
    dataframe['websource']='insidelacrosse'
    dataframe=drop_unnamed_columns(set_col_names_underscore_separated(set_col_names_lower(dataframe)))
    dfs_list.append(dataframe)
    
clubs_ranking_df=pd.concat(dfs_list)

# load

if WRITE_LOCAL_FLAG:
    clubs_ranking_df.to_csv(os.path.join(bronze_folder_path,bronze_file_name),index=False)
    print(f'{bronze_file_name} saved sucess into bronze folder')

if WRITE_DB_FLAG:
    host = '182.75.105.186'
    dev_port = '33096'
    dev_database = 'lacrosse-pre-dev'
    password='L@crosse753'
    user='lacrosse'
    try:
        print('loading data into database ... ')
        engine = create_engine('mysql+mysqlconnector://lacrosse:%s@182.75.105.186:33096/lacrosse-pre-dev' % quote(password))
        clubs_ranking_df.to_sql('clubs_ranking',engine,if_exists='append',index=False)
        print('success')
    except Exception as e:
        print(e)