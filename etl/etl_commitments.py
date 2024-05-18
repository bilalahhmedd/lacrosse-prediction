""" data processing script """
import os
import pandas as pd
import sys
sys.path.insert(1,os.path.join(os.path.realpath('__file__').split("lacrosse-prediction")[0],'lacrosse-prediction'))


from column_maps import american_select_names_map_dict,clublacrose_names_map_dict,laxnumbers_names_map_dict,toplax_names_map_dict,insidelacrosse_names_map_dict


from data_helper import extract_and_process_data_from_raw_folder,write_dfs_dict_to_folder
from data_helper import COLUMN_NAMES_REQUIRED
from data_helper import build_main_df_from_layer
from data_helper import drop_null_values_multiple_columns, drop_duplicates_multicols
from data_helper import process_data_from_bronze_to_silver,process_data_from_sliver_to_gold
from data_helper import to_camel_case

from config.config import DATA_FOLDER_PATH, category_config_dict
configuration = category_config_dict['commitments']
CATEGORY = configuration['category_name']
FOLDER_NAMES = configuration['websources_list']


division_map_dict = {
    'NCAA DI':'D1',
    'NCAA DII':'D2',
    'NCAA DIII':'D3',
    'D3 NCAA':'D3',
    'DI':'D1',
    'DII':'D2',
    'DIII':'D3'
}

position_map_dict = {
    'Midfield':'M',
    'Middie':'M',
    'Midflied':'M',
    'Midfiled':'M',
    'Defense':'D',
    'Attack':'A',
    'Goal':'G',
    'SSMF':'M',
    'FO/DRAW':'FO',
    'Face Off Specialist':'FO',
    'FOGO':'FO',
    'Faceoff':'FO',
    'Draw':'DRAW'
    }

def map_division_on_string(input_string:str,map:dict):
    for key, val in map.items():
        if key.replace(' ','').lower() == str(input_string).replace(' ','').lower():
            return val
    return input_string
    
def map_postition_on_string(input_string:str,map:dict):
    for key, val in map.items():
        if key.lower() in str(input_string).lower():
            return val
    return input_string
    
get_abv=lambda z: ''.join([str(x[0]).upper() for x in z.split(' ')]) if type(z)==str and len(z.split(' '))>1 else z

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
        elif folder_name == 'insidelacrosse':
             cols_map_dict=insidelacrosse_names_map_dict
                
        dfs_dict = extract_and_process_data_from_raw_folder(
            raw_folder_path,
            cols_map_dict,
            COLUMN_NAMES_REQUIRED
            )
        write_dfs_dict_to_folder(dfs_dict,bronze_folder_path) 


BRONZE_FOLDER_PATH = os.path.join(DATA_FOLDER_PATH,CATEGORY,'bronze')
SILVER_FOLDER_PATH = os.path.join(DATA_FOLDER_PATH,CATEGORY,'silver')
GOLD_FOLDER_PATH = os.path.join(DATA_FOLDER_PATH,CATEGORY,'gold')
OUTPUT_FILE_NAME = 'commitments.csv'
primary_columns = ['name','state','club','commitment_year','gender']
FINAL_STAGE_FOLDER_PATH = os.path.join(DATA_FOLDER_PATH,'final_stage')
if __name__ == "__main__":
    # process and extract data from scrapped raw to bronze
    process_raw_data_to_bronze(os.path.join(DATA_FOLDER_PATH,CATEGORY),FOLDER_NAMES)

    # process and extract data from bronze layer into silver layer
    process_data_from_bronze_to_silver(BRONZE_FOLDER_PATH,SILVER_FOLDER_PATH)
    # process and extract from silver to gold layer
    process_data_from_sliver_to_gold(SILVER_FOLDER_PATH,GOLD_FOLDER_PATH,OUTPUT_FILE_NAME)
    # process gold to final stage
    print('processing from gold to final stage')
    commitments_df = pd.read_csv(os.path.join(GOLD_FOLDER_PATH,OUTPUT_FILE_NAME))
    commitments_df['state']=commitments_df['state'].map(get_abv)
    commitment_year_outliers=[20225.0,2026.0]
    commitments_df=commitments_df.drop(commitments_df[commitments_df['commitment_year'].isin(commitment_year_outliers)].index)
    commitments_df = commitments_df[commitments_df['commitment_year'].notna()]
    commitments_df['commitment_year']=commitments_df['commitment_year'].astype(int)
    commitments_df['position']=commitments_df['position'].apply(lambda x: map_postition_on_string(x,position_map_dict))
    commitments_df['division']=commitments_df['division'].apply(lambda x: map_division_on_string(x,division_map_dict))
    camel_case_cols = [to_camel_case(col) for col in commitments_df.columns]
    commitments_df.columns=camel_case_cols
    commitments_df.to_csv(os.path.join(FINAL_STAGE_FOLDER_PATH,OUTPUT_FILE_NAME),index=False)
    print('success')


    