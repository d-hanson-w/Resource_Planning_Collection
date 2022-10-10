
# Utility Functions for Wartsila Resource Analysis
import pandas as pd
import plotly
import plotly.express as px
import asana    

import wa_access as wac

def get_api_rsc_tasks():
    #!! Remove before deployment 
    dh_wart_key = wac.get_asana_pat()
    wartsila_workspace = '8836488480224'
    resourcing_project_gid = '1202369325016953'

    #establish asana client
    client = asana.Client.access_token(dh_wart_key)

    task_fields = ['name','gid','assignee.name','start_on','due_on', 'custom_fields', 'projects.name']
    rsc_tasks2 = client.tasks.get_tasks_for_project(resourcing_project_gid, opt_fields=task_fields) # 'custom_fields'])

    # unpack generator
    rsc_task_list = list()
    for task in rsc_tasks2:
        rsc_task_list.append(task)
    return rsc_task_list

#------------------------------------------------
# Data Engineering 

#- Utility Functions for Resourcing Project Tasks
def get_project_type(task):
    for cf in task['custom_fields']:
        if cf['name'] == 'Project Type [ESO]':
            if cf['enum_value'] == None: 
                return 'no_type'
            return cf['enum_value']['name']
    else: 
        return 'no_type'


def get_role_eso_value(task): 
    for cf in task['custom_fields']:
        if cf['name'] == 'Role [ESO]':
            if cf['enum_value'] == None: 
                return 'no_role'
            return cf['enum_value']['name']
    else: 
        return 'no_role'


def get_assignee_name(task):
    assignee = task['assignee']
    if assignee == None:
        return 'unassigned'
    else: 
        return assignee['name']


def get_task_region(task):
    for cf in task['custom_fields']:
        if cf['name'] == 'Region [ESO]':
            if cf['enum_value'] == None:
                return 'no_region'
            else:    
                return cf['enum_value']['name']
    return 'no_region'


def get_eso_project_name(task):
    eso_pname = [p['name'] for p in task['projects'] if p['name'] != 'Delivery Team Resources']
    if not eso_pname: 
        return 'no_eso_project'
    elif len(eso_pname) == 1: 
        return eso_pname[0]
    else:
        return 'project_name_error'

def get_resource_allocation_percent(task):
    for cf in task['custom_fields']:
        if cf['name'] == 'Resource allocation percent [ESO]': 
            if cf['number_value'] == None: 
                return 0
            else: 
                return(cf['number_value'])

def convert_api_task_to_row(task):
    """
    combines processing steps for individual asana fields into one function
    """
    assignee_name = get_assignee_name(task)
    region = get_task_region(task)
    eso_project_name = get_eso_project_name(task)
    role = get_role_eso_value(task)
    project_type = get_project_type(task)
    resource_allocation = get_resource_allocation_percent(task)
    row = [assignee_name, region, eso_project_name, role, resource_allocation, task['start_on'], task['due_on'], task['gid'], task['name'], project_type]
    return row

def make_rsc_tasks_df(task_list):
    resource_rows = [convert_api_task_to_row(t) for t in task_list]
    resource_tasks_clean = pd.DataFrame(resource_rows, columns=['assignee_name', 'region', 'eso_project_name', 'role', 'resource_allocation_percent', 'start_date', 'due_date', 'gid', 'task_name', 'project_type']) 

    resource_tasks_clean['start_date'] =  pd.to_datetime(resource_tasks_clean['start_date'], format='%Y-%m-%d')
    resource_tasks_clean['due_date'] =  pd.to_datetime(resource_tasks_clean['due_date'], format='%Y-%m-%d')

    return resource_tasks_clean

def prep_task_display_table(df):
    df = df[['assignee_name', 'role', 'region', 'eso_project_name', 'start_date', 'due_date', 'project_type', 'resource_allocation_percent']]
    return df

def format_dates_as_strings(df):
    df['start_date'] = df['start_date'].dt.strftime("%Y-%m-%d")
    df['due_date'] = df['due_date'].dt.strftime("%Y-%m-%d")
    return df

def get_resource_project_data():
    """
    simplifies data acqiusiiton for this project to one function
    """
    rsc_task_list = get_api_rsc_tasks()
    df_tasks = make_rsc_tasks_df(rsc_task_list)
    df_tasks = prep_task_display_table(df_tasks)
    return df_tasks

#---------------------------------------------
# Analysis Functions

def filter_tasks_by_time_window(df, early_edge, late_edge): 
    df = df[(df['start_date'] <= late_edge) & (df['due_date'] >= early_edge)]
    df.sort_values(by=['start_date'])
    return df

def filter_tasks_by_role(df, roles_list):
    if not roles_list: 
        return df
    #roles_list = [item for sublist in roles_list for item in sublist]
    return df[df['role'].isin(roles_list)]

def filter_tasks_by_assignee(df, assignees_list):
    if not assignees_list:
        return df
    # roles_list [item for sublist in roles_list for item in sublist]
    return df[df['assignee_name'].isin(assignees_list)]

def filter_tasks_by_region(df, region_list):
    if not region_list:
        return df
    region_list = [item for sublist in region_list for item in sublist]
    return df[df['region'].isin(region_list)]

# todo
#def get_assigned_tasks(df):
    
def remove_unassigned_tasks(df):
    return df[df['assignee_name'] != 'unassigned']

#--------------------------
# Display Functions 

def make_unique_persons_per_role_counts_df(df):
    # counts number of po
    df_rc = remove_unassigned_tasks(df)
    df_rc = df_rc[df_rc['role'] != 'no_role']
    df_rc = df_rc.groupby(['role'])['assignee_name'].nunique().to_frame(name='role_count')
    df_rc.reset_index(inplace=True)
    df_rc = df_rc.sort_values(by=['role_count'], ascending=False)
    return df_rc

def make_unique_persons_per_role_counts_barchart(df):
    df_t = make_unique_persons_per_role_counts_df(df)
    fig = px.bar(df_t, x='role', y='role_count')
    return fig


#-------------------------
# Personnel Database Utilities

def get_persondb_api_data():
    dh_wart_key = "1/1202520154085312:4c57c9b9414b65f55d83d08d266cde29"
    wartsila_workspace = '8836488480224'
    personnel_db_gid = '1202677263177019'

    client = asana.Client.access_token(dh_wart_key)
    
    persondb_fields = ['name','gid','assignee.name', 'assignee.email', 'custom_fields']
    persondb_data = client.tasks.get_tasks_for_project(personnel_db_gid, opt_fields=persondb_fields) # 'custom_fields'])
                       
    pdb_task_list = list()
    for p in persondb_data:
        pdb_task_list.append(p)
    return pdb_task_list

def convert_api_persondb_to_row(task):
    assignee_name = get_assignee_name(task)
    region = get_task_region(task)
    role = get_role_eso_value(task)
    row = [assignee_name, role, region]
    return row

def make_persondb_df(pdb_list):
    pdb_rows = [convert_api_persondb_to_row(t) for t in pdb_list]
    df_pdb =  pd.DataFrame(pdb_rows, columns= ['person_name', 'role', 'region'])    
    
    df_pdb = df_pdb[df_pdb['person_name'] != 'unassigned']
    return df_pdb

def get_personnel_project_data():
    pdb_list = get_persondb_api_data()
    df_personnel = make_persondb_df(pdb_list)
    return df_personnel
    
def get_role_reference_df(df_master, role):
    return df_master[df_master['role'] == role]

def get_delivery_personnel_data():
    pdb_list = get_persondb_api_data()
    df_personnel = make_persondb_df(pdb_list)
    return df_personnel 

#-------------------------
# Global Portfolio Utilities

def extract_custom_field_vals(cf_list):
    vals_dict = dict()
    for c in cf_list: 
        c_type = c['type']
        if c_type == 'enum':
            c_val = c['enum_value']
            vals_dict[c['name']] = c_val['name'] if c_val is not None else 'none'

        elif c_type == 'text':
            c_val = c['text_value']
            vals_dict[c['name']] = c_val if c_val is not None else 'none'

        elif c_type == 'number':
            c_val = c['number_value']
            vals_dict[c['name']] = c_val if c_val is not None else 'none'
    return vals_dict


def get_global_portfolio_data():
    dh_wart_key = wac.get_asana_pat()
    wartsila_workspace = '8836488480224'
    global_portfolio_gid = '1201827339230998'

    #establish asana client
    client = asana.Client.access_token(dh_wart_key)

    #task_fields = ['name','gid','assignee.name','start_on','due_on', 'custom_fields', 'projects.name']
    #wrong - global_tasks = client.tasks.get_tasks_for_project(global_project_gid) #, opt_fields=task_fields) # 'custom_fields'])
    
    portfolio_fields = ['custom_fields']
    global_portfolio_cfs = client.portfolios.get_items_for_portfolio(global_portfolio_gid, opt_fields=portfolio_fields)
    
    global_portfolio_names = client.portfolios.get_items_for_portfolio(global_portfolio_gid)
    
    # unpack Portfolio Project Names API call
    global_portfolio_names_list = list()
    for p in global_portfolio_names:
        global_portfolio_names_list.append(p)
    df_global_pjs = pd.DataFrame(global_portfolio_names_list)
    
    # unpack Project Custom Fields API Call
    gpcl = list()
    for pc in global_portfolio_cfs:
        gpcl.append(pc)
    
    # extract custom field values
    pj_ext_cf_list = list()
    for g in gpcl:
        t_cfs = g['custom_fields']
        pj_dict = extract_custom_field_vals(t_cfs)
        pj_dict['gid'] = g['gid']
        pj_ext_cf_list.append(pj_dict)
    df_global_cfs = pd.DataFrame(pj_ext_cf_list)
    
    # merge Project Names df with Custom Fields df
    df_global = pd.merge(df_global_pjs, df_global_cfs, how='left', on='gid')
    
    return df_global