import os
import sys
sys.path.insert(1,os.path.join(os.path.realpath('__file__').split("lacrosse-prediction")[0],'lacrosse-prediction'))

import argparse
import pandas as pd
from etl.db_and_schema_helper import gen_table_schema_create_query
from etl.db_and_schema_helper import query_column_types_map
from etl.db_and_schema_helper import dev_db_cnn
from config.config import category_config_dict


parser = argparse.ArgumentParser(description='process configuration for given category')
parser.add_argument("--param",type=str,help="category parameter")
args = parser.parse_args()


parameter =args.param
configuration = category_config_dict[parameter]

csv_file_path = configuration['final_stage_csv_file']
db_table_name = configuration['db_table_name']
unique_db_index_cols = configuration['unique_db_index_cols']
input_df = pd.read_csv(csv_file_path)

try:
    schema_query=gen_table_schema_create_query(
        input_df,
        db_table_name,
        query_column_types_map,
        unique_index_columns=unique_db_index_cols
    )
    print(schema_query)
    print(f"creating table {db_table_name} in database ")
    dev_db_cnn._execute_query(schema_query)
    print("success")
except Exception as e:
    print("Failed")
    print(e)