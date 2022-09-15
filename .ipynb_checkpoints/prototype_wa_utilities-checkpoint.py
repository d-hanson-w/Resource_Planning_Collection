#-------------------------------------
#  
#-------------------------------------

import numpy as np 
import pandas as pd



#-------------------------------------
# Pipeline, Global, Workload Units 
#-------------------------------------

# Files: 
#
# Refactoring_Resource_Analysis_Modular.ipynb
#

def make_workload_units(df):
    """
    dex
    """
    df['workload_units'] = 1
    return df

def make_project_type_workload_columns(df):
    project_type_col = 'Project Type [ESO]'
    workload_column = 'workload_units'
    
    df['eeq_workload'] = np.where(df[project_type_col]=='EEQ', df[workload_column]*0.2, 0)
    df['epc_workload'] = np.where(df[project_type_col]=='EPC', df[workload_column]*0.3, 0)
    
    return df

def make_project_groups(df):
    df['eso_sub_project'] = df['eso_project_name']

    #---- Dagget 3 Group
    daggett3_mask = df['eso_project_name'].str.contains('Daggett 3')
    df.loc[df['eso_project_name'].str.contains('Daggett 3'), 'eso_project_name']='Daggett 3 Group'
    daggett3_group_mask = df['eso_project_name'] == 'Daggett 3 Group'
    
    dag_ce_mask = (daggett3_group_mask) & (df['role']=='Commissioning Engineer')
    dag_cm_mask = (daggett3_group_mask) & (df['role']=='Commissioning Manager')
    
    df.loc[dag_ce_mask, 'eso_project_name'] = 'Daggett 3 Group CmEng'
    df.loc[dag_cm_mask, 'eso_project_name'] = 'Daggett 3 Group CmMgr'
    #----
    
    return df

#-------------------------------------
#  Analysis Functions
#-------------------------------------
def make_single_person_summary(df):
    df['total_workload'] = df['eeq_workload'] + df['epc_workload']
    df.groupby('assignee_name').sum()
    return df

#-------------------------------------
# General Use
#-------------------------------------

def apply_filter_from_dict(df, filter_dict):
    """
    description: 
    applies multiple filters/masks to a pandas dataframe 
    (todo : add error handling if column name is not present)
    
    arguments
    --------
    df : pandas dataframe 
    filter_dict: dictionary
        key - must be column name in df argument
        value - the value to use for filtering column name (only one value allowed for now)
    """
    for key, value in filter_dict.items(): 
        df = df.loc[df[key] == value]
    return df


def make_list_of_workload_pre_analysis_filters(df):
    """
    description:
    prepares list of dicts. dicts to be used for filtering dataframes
    
    arguments
    ---------
    df : pandas dataframe to make filters
    
    note:
    dataframe must include three columns: 'role', 'region', '[ESO] Stage'
    """
    role_options = df['role'].unique().tolist()
    region_options = df['region'].unique().tolist()
    pipeline_options = df['[ESO] Stage'].unique().tolist()
    
    # make all filters
    filters_list_for_analysis = list()
    for region in region_options: 
        for role in role_options:
            for stage in pipeline_options:
                filters_list_for_analysis.append({'region': region, 'role': role, '[ESO] Stage': stage})
    
    return filters_list_for_analysis