import os
import sys
sys.path.insert(1,os.path.join(os.path.realpath('__file__').split("lacrosse-prediction")[0],'lacrosse-prediction'))

import argparse
import pandas as pd
from config.config import category_config_dict
from etl.db_and_schema_helper import engine
from etl.db_and_schema_helper import table_exist
from etl.db_and_schema_helper import load_df_to_db_table_fast, load_df_to_db_table_row_by_row
from etl.db_and_schema_helper import get_df_with_records_not_in_db_table



parser = argparse.ArgumentParser(description='process configuration for given category')
parser.add_argument("--param",type=str,help="category parameter")
args = parser.parse_args()


parameter =args.param
configuration = category_config_dict[parameter]


csv_file_path = configuration['final_stage_csv_file']
db_table_name = configuration['db_table_name']
unique_db_index_cols = configuration['unique_db_index_cols']
input_df = pd.read_csv(csv_file_path)

if not table_exist(db_table_name,engine):
    print(f'{db_table_name} table not exist in database')
else:
    df_to_load = get_df_with_records_not_in_db_table(input_df,db_table_name,engine,unique_db_index_cols)
    if load_df_to_db_table_fast(
        df_to_load,
        db_table_name,
        engine
        ):
        print(f"data pushed to database table {db_table_name} using fast method")
    else:
        load_df_to_db_table_row_by_row(
            df_to_load,
            db_table_name,
            engine,
        )

