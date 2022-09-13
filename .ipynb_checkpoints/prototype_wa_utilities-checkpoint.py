#-------------------------------------
#  
#-------------------------------------





#-------------------------------------
# Pipeline, Global, Workload Units 
#-------------------------------------

# Files: 
#
# Refactoring_Resource_Analysis_Modular.ipynb
#
#
#


def add_workload_units(df):
    """
    dex
    """
    df['workload_units'] = 1
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