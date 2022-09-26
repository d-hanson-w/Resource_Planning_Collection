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
    Purpose : Assigne workload dummy data - later to be replaced by real data
    """
    df['workload_units'] = 1
    return df

def make_num_projects_column(df):
    """
    Purpose : Add column that can later be used in aggregate numerical calculations
    """
    df['num_projects'] = 1
    return df

def make_assigned_status_column(df):
    """
    Purpose : Add column to use for eaisly filtering Assigned and Unassigned Tasks
    """
    #---------
    general_unassigned = (df['assignee_name'] == 'unassigned')
    amer_unassigned = (df['assignee_name'] == 'Antti Pitkänen')
    
    options = [general_unassigned, amer_unassigned]
    outputs = ['unassigned', 'unassigned']
    df['assigned_status'] = np.select(options, outputs, 'assigned')
    return df

def make_project_type_multiplier_columns(df):
    
    epc_multiplier_dict = {
    'Commissioning Manager': 1.0,
    'Commissioning Engineer' : 1.0, 
    'System Engineer' : 0.5, 
    'Solution Engineer' : 0.4, 
    'CPE Civil' : 0.2, 
    'CPE Electrical' : 0.5, 
    }
     
    eeq_multiplier_dict = {
    'Commissioning Manager': 1.0,
    'Commissioning Engineer' : 1.0, 
    'System Engineer' : 0.25, 
    'Solution Engineer' : 0.2, 
    'CPE Civil' : 0.1, 
    'CPE Electrical' : 0.1, 
    }    
    
    eeq_options = [(df['role']==role) for role in eeq_multiplier_dict.keys()]
    eeq_outputs = eeq_multiplier_dict.values()
    
    epc_options = [(df['role']==role) for role in epc_multiplier_dict.keys()]
    epc_outputs = epc_multiplier_dict.values()
    
    df['eeq_workload_multiplier'] = np.select(eeq_options, eeq_outputs, 1)
    df['epc_workload_multiplier'] = np.select(epc_options, epc_outputs, 1)
    return df
        


def make_project_type_workload_columns(df):
    project_type_col = 'Project Type [ESO]'
    workload_column = 'workload_units'
    
    # Workload Multipliers
    multiplier_eeq = 0.2
    multiplier_epc = 0.3
    
    # adding project type data columns
    df['num_eeq_projects'] = np.where(df[project_type_col].str.contains('EEQ'), 1, 0)
    df['num_epc_projects'] = np.where(df[project_type_col].str.contains('EPC'), 1, 0)
    df['num_no_type_projects'] = np.where(df[project_type_col]=='no_type', 1, 0)
    
    # df['eeq_workload'] = np.where(df[project_type_col].str.contains('EEQ'), df[workload_column]*multiplier_eeq, 0)
    # df['epc_workload'] = np.where(df[project_type_col].str.contains('EPC'), df[workload_column]*multiplier_epc, 0)
    
    df = make_project_type_multiplier_columns(df)
    df['eeq_workload'] = np.where(df[project_type_col].str.contains('EEQ'), df[workload_column]*df['eeq_workload_multiplier'], 0)
    df['epc_workload'] = np.where(df[project_type_col].str.contains('EPC'), df[workload_column]*df['epc_workload_multiplier'], 0)
                                  
    return df

def make_comparison_columns_for_expanded(df):
    """
    
    """
    
    
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
    df = df.copy()
    df['total_workload'] = df['eeq_workload'] + df['epc_workload']
    df = df.groupby('assignee_name').sum()
    return df

def make_unassigned_day_summary(df):
    df = df.copy()
    df = df.groupby('date').sum()
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
    assigned_options = df['assigned_status'].unique().tolist()
    
    # make all filters
    filters_list_for_analysis = list()
    for assigned_status in assigned_options:
        for region in region_options: 
            for role in role_options:
                for stage in pipeline_options:
                    filters_list_for_analysis.append({'region': region, 
                                                      'role': role, 
                                                      '[ESO] Stage': stage, 
                                                      'assigned_status': assigned_status})
    
    return filters_list_for_analysis