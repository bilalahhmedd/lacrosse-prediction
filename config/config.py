import os
import sys
from pathlib import Path

PROJECT_ROOT_PATH = os.path.join(os.path.realpath('__file__').split("lacrosse-prediction")[0],'lacrosse-prediction')
DATA_FOLDER_PATH = os.path.join(PROJECT_ROOT_PATH,'data')
FINAL_STAGE_FOLDER_PATH = os.path.join(DATA_FOLDER_PATH,'final_stage')
INPUT_CONFIG_PATH = os.path.join(PROJECT_ROOT_PATH,'config','input')
TESTING_CATEGORY_NAME='testing'

category_config_dict={
    'testing':{
        'category_name':'testing',
        'websources_list':['websrource1','websource2'],
        'final_stage_csv_file':os.path.join(FINAL_STAGE_FOLDER_PATH,'testing.csv'),
        'db_table_name':'testing',
        'unique_db_index_cols':['event','event_date','gender','added','web_source'],
        
    },
    'commitments':{
        'category_name':'commitments',
        'websources_list':['americanselectlacrosse','clublacrosse','laxnumbers','toplaxrecruits','insidelacrosse'],
        'scrapper_output_folder_path':os.path.join(DATA_FOLDER_PATH,'commitments','raw'),
        'final_stage_csv_file':os.path.join(FINAL_STAGE_FOLDER_PATH,'commitments.csv'),
        'db_table_name':'testingCommitments',
        'unique_db_index_cols':['name','commitmentYear','highSchool','club','position','division','webSource','gender'],
        
    },
    'camps_and_clinics':{
        'category_name':'camps_and_clinics',
        'websources_list':['airtable','mass_elite'],
        'scrapper_output_folder_path':os.path.join(DATA_FOLDER_PATH,'camps_and_clinics','raw'),
        'final_stage_csv_file':os.path.join(FINAL_STAGE_FOLDER_PATH,'camps_and_clinics.csv'),
        'db_table_name':'testingCamps_and_Clinics',
        'unique_db_index_cols':['event','eventDate','gender','added','webSource'],
        
    },
    'clubs_ranking':{
        'category_name':'clubs_ranking',
        'websources_list':['insidelacrosse'],
        'status_config_folder_path':os.path.join(INPUT_CONFIG_PATH,'clubs_ranking'),
        'scrapper_output_folder_path':os.path.join(DATA_FOLDER_PATH,'clubs_ranking','raw'),
        'final_stage_csv_file':os.path.join(FINAL_STAGE_FOLDER_PATH,'clubs_ranking.csv'),
        'db_table_name':'testingClubs_Ranking',
        'unique_db_index_cols':['club','gender','class','season','webSource'],
        
    },
    'colleges_ranking':{
        'category_name':'colleges_ranking',
        'websources_list':['lacrossereference','laxmath','ncaa'],
        'scrapper_output_folder_path':os.path.join(DATA_FOLDER_PATH,'colleges_ranking','raw'),
        'final_stage_csv_file':os.path.join(FINAL_STAGE_FOLDER_PATH,'colleges_ranking.csv'),
        'db_table_name':'testingcolleges_ranking',
        'unique_db_index_cols':['rank','team','powerRating','gender','division','webSource','winRatio'],
        
    },
    'schools_ranking':{
        'category_name':'schools_ranking',
        'websources_list':['lacrossereference','laxmath','ncaa'],
        'scrapper_output_folder_path':os.path.join(DATA_FOLDER_PATH,'schools_ranking','raw'),
        'final_stage_csv_file':os.path.join(FINAL_STAGE_FOLDER_PATH,'schools_ranking.csv'),
        'db_table_name':'testingSchools_ranking',
        'unique_db_index_cols':['rank','team','powerRating','gender','webSource','winRatio'],
        
    },


}

camps_and_clinics_paths_dict = {
    'folder_path':os.path.join(DATA_FOLDER_PATH,'camps_and_clinics'),
    'raw_folder_path':os.path.join(DATA_FOLDER_PATH,'camps_and_clinics','raw'),
    'bronze_folder_path':os.path.join(DATA_FOLDER_PATH,'camps_and_clinics','bronze'),
    'silver_folder_path':os.path.join(DATA_FOLDER_PATH,'camps_and_clinics','silver'),
    'gold_folder_path':os.path.join(DATA_FOLDER_PATH,'camps_and_clinics','gold'),
    }

clubs_ranking_paths_dict = {
    'folder_path':os.path.join(DATA_FOLDER_PATH,'clubs_ranking'),
    'raw_folder_path':os.path.join(DATA_FOLDER_PATH,'clubs_ranking','raw'),
    'bronze_folder_path':os.path.join(DATA_FOLDER_PATH,'clubs_ranking','bronze'),
    'silver_folder_path':os.path.join(DATA_FOLDER_PATH,'clubs_ranking','silver'),
    'gold_folder_path':os.path.join(DATA_FOLDER_PATH,'clubs_ranking','gold'),
    }

colleges_ranking_paths_dict = {
    'folder_path':os.path.join(DATA_FOLDER_PATH,'colleges_ranking'),
    'raw_folder_path':os.path.join(DATA_FOLDER_PATH,'colleges_ranking','raw'),
    'bronze_folder_path':os.path.join(DATA_FOLDER_PATH,'colleges_ranking','bronze'),
    'silver_folder_path':os.path.join(DATA_FOLDER_PATH,'colleges_ranking','silver'),
    'gold_folder_path':os.path.join(DATA_FOLDER_PATH,'colleges_ranking','gold'),
    }

schools_ranking_paths_dict = {
    'folder_path':os.path.join(DATA_FOLDER_PATH,'schools_ranking'),
    'raw_folder_path':os.path.join(DATA_FOLDER_PATH,'schools_ranking','raw'),
    'bronze_folder_path':os.path.join(DATA_FOLDER_PATH,'schools_ranking','bronze'),
    'silver_folder_path':os.path.join(DATA_FOLDER_PATH,'schools_ranking','silver'),
    'gold_folder_path':os.path.join(DATA_FOLDER_PATH,'schools_ranking','gold'),
    }

commitments_paths_dict = {
    'folder_path':os.path.join(DATA_FOLDER_PATH,'commitments'),
    'raw_folder_path':os.path.join(DATA_FOLDER_PATH,'commitments','raw'),
    'bronze_folder_path':os.path.join(DATA_FOLDER_PATH,'commitments','bronze'),
    'silver_folder_path':os.path.join(DATA_FOLDER_PATH,'commitments','silver'),
    'gold_folder_path':os.path.join(DATA_FOLDER_PATH,'commitments','gold'),
    }
