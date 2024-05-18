""" this module provides method to create schemas for tables and database for mysql db"""

# testing if query works in database table
import os
import sys
sys.path.insert(1,os.path.join(os.path.realpath('__file__').split("lacrosse-prediction")[0],'lacrosse-prediction'))

import pandas as pd
import mysql.connector
from sqlalchemy import create_engine
from urllib.parse import quote_plus

from config.db_creds import dev_db_creds


dev_db_host=dev_db_creds['host']
dev_db_user=dev_db_creds['user']
dev_db_password=dev_db_creds['password']
dev_db = dev_db_creds['database']
dev_db_port = dev_db_creds['port']
dev_db_cnn = mysql.connector.connect(
    host =dev_db_host,
    user = dev_db_user,
    password = dev_db_password,
    db = dev_db,
    port=dev_db_port
    )
cursor = dev_db_cnn.cursor()
engine=create_engine(url=f"mysql+pymysql://{dev_db_user}:%s@{dev_db_host}:{dev_db_port}/{dev_db}" % quote_plus(dev_db_password))

query_column_types_map = {'INTEGER':'INT DEFAULT NULL','TEXT':'VARCHAR(90) DEFAULT NULL','REAL':'DOUBLE DEFAULT NULL'}

def replace_using_map(
        input_string:str,
        input_map:dict
        ):
    """ change column maps to query"""
    
    for key, val in input_map.items():
        input_string=input_string.replace(key,val)
    return input_string

def append_to_query(query:str,input_string:str):
    part_0,part_1=query.split('\n)')
    return part_0+',\n'+f"{input_string}"+'\n)'

def gen_validated_unique_index(input_df,index_cols=[],index_name='unique_index2'):
    """ validates and returns unique index """
    output_string = f' UNIQUE KEY `{index_name}` ( '
    for col in index_cols:
        if col not in input_df.columns:
            return None
        output_string=output_string +f' `{str(col)}`,'
    output_string=output_string[:-1]+') '
    return output_string

def add_primary_key_to_query(
        input_query:str,
        key_col:str,
        key_col_type='INT',
        constraints='NOT NULL AUTO_INCREMENT',
        ):
    """append primary key part into query"""
    part1,part2=input_query.split('(\n')
    input_query=part1+'(\n'+f'"{key_col}" {key_col_type} {constraints},\n'+part2
    input_query=append_to_query(input_query,f'PRIMARY KEY ("{key_col}")')
    return input_query

def gen_table_schema_create_query(
        input_df:object,
        table_name:str,
        column_types_map={},
        primary_key_col='id',
        primary_key_col_type="INT",
        primary_key_constraints='NOT NULL AUTO_INCREMENT',
        unique_index_columns=[],
        unique_index_name='UNIQUE_INDEX2'
    ):
    """ method to create table schema query """
    query = pd.io.sql.get_schema(input_df,table_name)
    query=replace_using_map(query,column_types_map)
    query=add_primary_key_to_query(
        query,
        primary_key_col,
        primary_key_col_type,
        primary_key_constraints
        )
    if len(unique_index_columns)> 0:
        unique_index_constraint = gen_validated_unique_index(input_df,unique_index_columns,unique_index_name)
        if unique_index_constraint is not None:
            query = append_to_query(query,unique_index_constraint)
    query = query.replace('"','`')
    return f"""{query}"""
    
def table_exist(
        table_name:str,
        conn:object
        ):
    """ check if table exists in database """
    query_if_table_exist = f"select * from information_schema.tables where table_name='{table_name}'"
    return pd.read_sql_query(query_if_table_exist,conn).shape[0]==1

def push_df_to_db(
        input_df:object,
        db_table_name:str,
        db_connection:object,
        index=False,
        ):
    """ push dataframe to database """
    input_df.to_sql(name=db_table_name,con= db_connection,index=index)

def get_df_with_records_not_in_db_table(
        input_df:object,
        target_table_name:str,
        db_connection:object,
        unique_key_cols=[]
        ):
        """method to compared input df with db table and return unique datframe with records not present in db table"""
        db_table_df=pd.read_sql(f'select * from {target_table_name}',db_connection)
        db_table_df_wo_id = db_table_df[db_table_df.columns[1:]]
        # print("data extraction from target table success")
        input_df=input_df.astype(db_table_df_wo_id.dtypes.to_dict())
        # print("input dataframe dolumn types updated as per target Table")
        if len(unique_key_cols) > 0:
                input_df=input_df.drop_duplicates(unique_key_cols)
        else:
                input_df=input_df.drop_duplicates()
        indexes_series=input_df.merge(db_table_df_wo_id.drop_duplicates(),how='left',indicator=True)['_merge']=='left_only'
        dataframe_to_load=input_df.iloc[indexes_series[indexes_series].index]
        # print("input dataframe deduplication done")
        print(f'unique records to load: {dataframe_to_load.shape[0]}')
        return dataframe_to_load


def load_df_to_db_table_row_by_row(
        input_df:object,
        target_table_name:str,
        db_connection:object,
        index=False
    ):
    """ laod dataframe into target table row by row"""
    # iterate through dataframe and append data into sql database
    skipped_rows_count=0
    success_rows_count=0
    for i in range(len(input_df)):
        try:
            input_df.iloc[i:i+1].to_sql(name=target_table_name,if_exists='append',con = db_connection,index=index)
            success_rows_count+=1
            if success_rows_count%5==0:
                print(f'{success_rows_count} records added success ... ')
        except Exception as e:
            skipped_rows_count+=1
            if skipped_rows_count%5==0:
                print(f'records skipped: {skipped_rows_count}')
            print(e)
    
    print(f'{success_rows_count} records added success')
    print(f'records skipped: {skipped_rows_count}')
    return True

def load_df_to_db_table_fast(
        input_df:object,
        target_table_name:str,
        db_connection:object,
        index=False,
        ):
    """ load dataframe into database table fast """
    try:
        if input_df.shape[0]>0:
            print(f"loading data into table {target_table_name} ")
            input_df.to_sql(target_table_name,db_connection,if_exists='append',index=index)
            print('success')
            return True
    except Exception as e:
        print(e)
        return False
    return True


def load_df_to_database(
        input_df:object,
        db_table_name:str,
        db_connection:object,
        unique_index_cols=[],
        index=False,

        ):
    """ load dataframe to database """
    if not table_exist(db_table_name,db_connection):
        print(f"{db_table_name} does not exist ")
    else:
        df_to_load = get_df_with_records_not_in_db_table(input_df,db_table_name,db_connection,unique_index_cols)
        if load_df_to_db_table_fast(df_to_load,db_table_name,db_connection):
            print(f'data loaded with fast method')
        else:
            print(f"opting method to push data row by row")
            load_df_to_db_table_row_by_row(df_to_load,db_table_name,db_connection)