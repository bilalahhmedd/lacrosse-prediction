""" module to contain data processing methods """

# import required libs
import os
import re
import pandas as pd

# data extraction methods
COLUMN_NAMES_REQUIRED = [
    'name',
    'high_school',
    'state',
    'commitment_year',
    'club',
    'club_year',
    'club_rank',
    'gender',
    'college',
    'position',
    'height',
    'weight'
    ]

def get_csv_dataframes_from_folder(folder_path:str):
    """ method to read multiple csv files inside single folder"""
    output_dfs_dict = {}
    for file in os.listdir(folder_path):
        output_dfs_dict[file]=pd.read_csv(os.path.join(folder_path,file))
    return output_dfs_dict

# data processing methods
def map_col_names(
    input_df:object,
    col_names_map_dict:dict
    ):
    """ map scrapped csv col names to required column names list """
    return input_df.rename(columns=col_names_map_dict)

def merge_first_last(
    input_df:object,
    cols_to_merge:list,
    new_col_name = 'name'
    ):
    """ merge First Name, Last Name column into one column called name """
    output_df = input_df
    for col in cols_to_merge:
        output_df[col]=output_df[col].astype(str)
    output_df[new_col_name]= output_df[cols_to_merge].agg(' '.join, axis=1)
    output_df = output_df.drop(cols_to_merge,axis=1)
    return output_df

def set_col_names_lower(input_df:object):
    """ lower column names """
    return input_df.set_axis([x.lower() for x in list(input_df.columns)],axis=1)

def set_col_names_underscore_separated(input_df:object):
    """make column names single underscore separated"""
    return input_df.set_axis([re.sub(' +','_',x) for x in list(input_df.columns)],axis=1)

def drop_unnamed_columns(input_df):
    """ drop unnamed columns from dataframe """
    df_ = input_df
    df_ = df_.loc[:, ~df_.columns.str.contains('^unnamed')]
    return df_

def refactor_col_names(
    input_df:object,
    col_names_map_dict:dict,
    source_website='laxnumbers'
    ):
    """
    refactor column names to keep consistency
    and follow naming convention (lowercase, 
    underscore-separated 
    """
    output_df = input_df
    if source_website == 'americanselectlacrosse':
        output_df = merge_first_last(
            output_df,
            ['First','Last'],
            'name'
        )
    elif source_website == 'toplaxrecruits':
        output_df = merge_first_last(
            output_df,
            ['First Name','Last Name'],
            'name'
        )
    output_df=map_col_names(output_df,col_names_map_dict)
    output_df=set_col_names_lower(output_df)
    output_df=set_col_names_underscore_separated(output_df)
    return output_df

# naming files as per standard convention

def name_alpha_numeric_ended(file_name:str):
    """ end name with alphanumeric character only """
    if file_name[-1].isalnum():
        return file_name
    file_name=file_name[:-1]
    return name_alpha_numeric_ended(file_name)

def revamp_file_name(file_name:str):
    """ rename csv file according to convention """
    # lower name,
    # replace space and dash with underscore,
    # remove comma, colon, semicolon etc from name,
    # last character should be alphabat,
    file_name= file_name.lower().translate(str.maketrans({' ':'_','-':'_','"':'',"'":""}))
    name, ext = os.path.splitext(file_name)
    name= name_alpha_numeric_ended(name)
    file_name=name+ext
    file_name= file_name.replace('mens','men')
    return file_name

# populate gender column methods
boys_identifiers = ['boy','boys','man','men']
girl_identifiers = ['girl','girls','woman','women','commitment']

def identify_gender(file_name_string):
    """ method to identify gender for given file name """
    for y in boys_identifiers:
        if y in file_name_string and "wo"+y not in file_name_string:
            return 'boy'
    for x in girl_identifiers:
        if x in file_name_string:
            return 'girl'
    return None

def populate_df_with_gender(input_df:object,input_string:str):
    """ populate dataframe with new column gender """
    output_df = input_df
    output_df['gender']=identify_gender(input_string)
    return output_df

# deal with commitment year

def get_year_from_string(file_name:str):
    """ detects year from file name string """
    try:
        output = re.match(r'.*([1-3][0-9]{3})', file_name).group(1)
        return output
    except:
        print('year not found in file name: '+str(file_name))
        return None

def populate_commitment_year_from_filename(input_df,file_name:None):
    """ populate dataframe using  year from filename string """
    year = get_year_from_string(file_name)
    output_df = input_df
    output_df['commitment_year']=year
    return output_df

# raw to bronze layer processing methods

def write_dfs_dict_to_folder(
    input_dict:dict,
    folder_path:str
    ):
    """ writes dataframes in dict into target folder """
    for file_name, df in input_dict.items():
        df.to_csv(os.path.join(folder_path,file_name),index=False)
        print(file_name+' written to '+folder_path+ ' success ... ')
    return None

def extract_and_process_data_from_raw_folder(
        raw_folder_path:str,
        column_names_map_dict:object,
        column_names_required:list
    ):
    """ read csv files and extract and transform data ready for bronze layer """
    csv_dfs_dict = get_csv_dataframes_from_folder(raw_folder_path)
    print('csv dfs read success')
    source_website_name = raw_folder_path.split('/')[-1]
    print(source_website_name)
    bronze_csv_dfs_dict = {}
    for raw_file_name, df in csv_dfs_dict.items():
        if "americanselect" in raw_folder_path:
            # populate commitment year from file name
            df=populate_commitment_year_from_filename(df,raw_file_name)
        bronze_file_name = revamp_file_name(raw_file_name)
        # refactor column names
        df_refactored = refactor_col_names(df,column_names_map_dict,source_website_name)
        # populate dataframe with gender column
        df_refactored = populate_df_with_gender(df_refactored,raw_file_name)
        # populate missing columns from required list with None value default
        missing_columns = list(set(column_names_required)-set(list(df_refactored.columns)))
        df_refactored[missing_columns]=None
        df_refactored['web_source']=source_website_name
        df_refactored = drop_unnamed_columns(df_refactored)
        bronze_csv_dfs_dict[bronze_file_name]=df_refactored
        
    return bronze_csv_dfs_dict

def build_main_df_from_layer(folder_path:str):
    """ build dataframe from root folder path"""
    # layer means parent folder where all webfolders reside. i.e data/raw/ or data/bronze
    dfs_list =[]
    for folder in os.listdir(folder_path):
        dfs_dict = get_csv_dataframes_from_folder(os.path.join(folder_path,folder))
        dfs_folder_level_list = []
        for _,df in dfs_dict.items():
            dfs_folder_level_list.append(df)
        folder_level_dataframe = pd.concat(dfs_folder_level_list)
        dfs_list.append(folder_level_dataframe)
    main_dataframe = pd.concat(dfs_list)
    return main_dataframe

def drop_null_values_multiple_columns(
    input_df,
    columns:list):
    """ drops all rows where any single entry is null for given list of columns """
    output_df = input_df.copy()
    for col in columns:
        output_df = output_df.loc[output_df[col].isnull()==False]
    return output_df

def drop_duplicates_multicols(input_df:object,columns:list):
    """ drop duplicates for multiple columns in dataframe"""
    return input_df.drop_duplicates(subset=columns).reset_index(drop=True)
