""" data processing script """
import os

from column_maps import american_select_names_map_dict,clublacrose_names_map_dict,laxnumbers_names_map_dict,toplax_names_map_dict

from data_helper import extract_and_process_data_from_raw_folder,write_dfs_dict_to_folder
from data_helper import COLUMN_NAMES_REQUIRED
from data_helper import build_main_df_from_layer
from data_helper import drop_null_values_multiple_columns, drop_duplicates_multicols

def process_raw_data_to_bronze(data_folder_path:str,folder_names:list):
    """process raw scrapped data and copy data to bronze layer"""
    for folder_name in folder_names:

        raw_folder_path = os.path.join(data_folder_path,'raw',folder_name)
        bronze_folder_path = os.path.join(data_folder_path,'bronze',folder_name)
        if not os.path.exists(bronze_folder_path):
            os.makedirs(bronze_folder_path)
            print(bronze_folder_path+' created success ... ')
        
        if folder_name == 'americanselectlacrosse':
                cols_map_dict = american_select_names_map_dict
        elif folder_name == 'clublacrosse':
                cols_map_dict = clublacrose_names_map_dict
        elif folder_name == 'laxnumbers':
                cols_map_dict = laxnumbers_names_map_dict
        elif folder_name == 'toplaxrecruits':
                cols_map_dict = toplax_names_map_dict
                
        dfs_dict = extract_and_process_data_from_raw_folder(
            raw_folder_path,
            cols_map_dict,
            COLUMN_NAMES_REQUIRED
            )
        write_dfs_dict_to_folder(dfs_dict,bronze_folder_path) 

DATA_FOLDER_PATH = '../data/'
FOLDER_NAMES = ['americanselectlacrosse','clublacrosse','laxnumbers','toplaxrecruits']
BRONZE_FOLDER_PATH = '../data/bronze/'
SILVER_FOLDER_PATH = '../data/silver/'

primary_columns = ['name','state','club','commitment_year','gender']

if __name__ == "__main__":
#     process and extract data from scrapped raw to bronze
#     process_raw_data_to_bronze(DATA_FOLDER_PATH,FOLDER_NAMES)

#     process and extract data from bronze layer into silver layer
    bronze_main_df = build_main_df_from_layer(BRONZE_FOLDER_PATH)
    print(bronze_main_df.shape)
    print('bronze main dataframe created for '+BRONZE_FOLDER_PATH)
    silver_df = drop_null_values_multiple_columns(bronze_main_df,primary_columns)
    print('dropped entries for key columns')
    silver_df = drop_duplicates_multicols(silver_df,primary_columns)
    print('dropped duplicates based on key columns')
    if not os.path.exists(SILVER_FOLDER_PATH):
        os.makedirs(SILVER_FOLDER_PATH)
        print('silver folder created success '+SILVER_FOLDER_PATH)

    silver_df_path = os.path.join(SILVER_FOLDER_PATH,'commitments.csv')
    silver_df.to_csv(silver_df_path,index=False)
    print('silver dataframe created success: '+silver_df_path + '\nshape: '+str(silver_df.shape))

    