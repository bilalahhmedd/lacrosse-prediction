import os
import sys
sys.path.insert(1,os.path.join(os.path.realpath('__file__').split("lacrosse-prediction")[0],'lacrosse-prediction'))
import pandas as pd
from etl.data_helper import create_none_existing_folder,get_csv_dataframes_from_folder,revamp_file_name
from etl.data_helper import set_col_names_lower, set_col_names_underscore_separated,drop_unnamed_columns
from etl.data_helper import map_col_names

FOLDERS_FOR_ETL = ['raw','bronze','silver','gold']

def create_data_folders(
        category_name:str,
        path:str,
        source_folders=[],
        ):
    """ create data folders for given category """
    create_none_existing_folder(os.path.join(path,category_name))
    path=os.path.join(path, category_name)
    for folder in FOLDERS_FOR_ETL:
        if folder in ['raw','bronze']:
            if len(source_folders) >0:
                for source_folder in source_folders:
                    create_none_existing_folder(os.path.join(path,folder,source_folder))
            else:
                create_none_existing_folder(os.path.join(path,folder))
        else:
            create_none_existing_folder(os.path.join(path,folder))
            
    return True

def process_raw_to_bronze(
        raw_folder_path:str,
        bronze_folder_path:str,
        column_map_flag=False,
        columns_map={}
        ):
    """ process data from raw folder into bronze folder """
    # extract data from raw raw folder path
    for folder in os.listdir(raw_folder_path):
        dfs_dict = get_csv_dataframes_from_folder(os.path.join(raw_folder_path,folder))
        for file_name, df in dfs_dict.items():
            target_bronze_folder = os.path.join(bronze_folder_path,folder)
            create_none_existing_folder(target_bronze_folder)
            new_file_name = revamp_file_name(file_name)
            print(f'procesisng dataframe for {file_name} ')
            if column_map_flag:
                df = map_col_names(df,columns_map)
                print(f'column mapping applied success')
            df = drop_unnamed_columns(set_col_names_underscore_separated(set_col_names_lower(df)))
            print(f'saving data file {new_file_name} in {target_bronze_folder}')
            df.to_csv(os.path.join(target_bronze_folder,new_file_name),index=False)
            print('success')
    return True

def process_bronze_to_silver(
        bronze_folder_path:str,
        silver_folder_path:str,
        list_of_functions=[]
        ):
    """ process data from bronze layer into sivler layer """
    for folder in os.listdir(bronze_folder_path):
        print(f'processing {folder} ')
        dfs_list = []
        dfs_dict = get_csv_dataframes_from_folder(os.path.join(bronze_folder_path,folder))
        for file_name , df in dfs_dict.items():
                # add folder name as websource
                if 'web_source' not in df.columns:
                    df['web_source']=folder
                    print(f'column web_source added in dataframe for {file_name}')
                dfs_list.append(df)
                print(file_name)
        dataframe = pd.concat(dfs_list)        
        print(f'saving {folder}.csv in {silver_folder_path}')
        dataframe.to_csv(os.path.join(silver_folder_path,folder+'.csv'),index=False)
        print('success')
    return True

def process_silver_to_gold(
        silver_folder_path:str,
        gold_folder_path:str,
        output_file_name:str
        ):
    """ process data from silver folder to gold folder """
    print(f'extract data from {silver_folder_path}')
    dfs_dict = get_csv_dataframes_from_folder(silver_folder_path)
    dfs_list = []
    for file_name, df in dfs_dict.items():
        print(file_name)
        dfs_list.append(df),
    dataframe = pd.concat(dfs_list)
    print(f'saving {output_file_name} in {gold_folder_path}')
    dataframe.to_csv(os.path.join(gold_folder_path,output_file_name),index=False)
    print(f'success')
    return True
