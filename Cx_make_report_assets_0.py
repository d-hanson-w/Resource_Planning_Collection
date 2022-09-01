import plotly.express as px
import pandas as pd
import numpy as np
from datetime import date, datetime, timezone
import pytz

from pathlib import Path  

import wartsila_asana_utils as wa
import asana


#------------------------------------------
# Utility Functions - todo separate into separte module

def get_num_unique_pjs_by_window(df, start_date, end_date):
    df_out = wa.filter_tasks_by_time_window(df, start_date, end_date)['eso_project_name']
    pj_count = df_out.nunique()
    return pj_count

def get_list_unique_pjs_by_window(df, start_date, end_date):
    df_out = wa.filter_tasks_by_time_window(df, start_date, end_date)['eso_project_name']
    pj_list = df_out.unique().tolist()
    return pj_list

def get_num_active_cxs(df, start_date, end_date):
    df_out = wa.filter_tasks_by_time_window(df, start_date, end_date)['assignee_name']
    person_count = df_out.nunique()
    return person_count

def get_list_active_cxs(df, start_date, end_date):
    df_out = wa.filter_tasks_by_time_window(df, start_date, end_date)['assignee_name']
    person_list = df_out.unique().tolist()
    return person_list

def get_list_available_cx_persons(df, start_date, end_date):
    active_cx = get_list_active_cxs(df, start_date, end_date)
    cx_avail = list(set(list_all_cx_persons) - set(active_cx))
    return cx_avail

def make_personnel_analysis_df(df_all, df_as, df_unas, max_prsn_available):
    """
    Parameters
    ----------
    df_all : pandas dataframe
        all tasks to be used in this analysis
    df_as : pandas dataframe
        tasks that have an assignee
    df_unas : pandas dataframe
        tasks with no assignee (or by filtering to be treated as having to assignee)
    max_prsn_available: int
        maximum number of people capable of acting in role/capacity of interest
        (note: # todo - upgrade max_prsn_available static value with a schedule of people hired/employed over time)
    """
    
    ## Daily Time Reference 
    start = pd.Timestamp('1/1/2022')
    end = pd.Timestamp('1/1/2024')
    tindex = pd.date_range(start, end)
    df_dater2 = pd.DataFrame(tindex, columns=['date'])
    
    analysis_list = [{
        'date' : r['date'],
        'num_total_projects' : get_num_unique_pjs_by_window(df_all, r['date'], r['date']),
        # 'num_total_tasks' : len(df_all.index)
        'num_projects_no_assignee' : get_num_unique_pjs_by_window(df_unas, r['date'], r['date']),
        'num_persons_occupied' : get_num_unique_pjs_by_window(df_as, r['date'], r['date']),                                                                 
    } for _, r in df_dater2.iterrows()]
    
    df = pd.DataFrame(analysis_list)
    
    # todo - upgrade max_prsn_available static value with a schedule of people hired/employed over time
    df['num_persons_available'] = max_prsn_available - df['num_persons_occupied']
    df['num_persons_supply'] = df['num_persons_available'] - df['num_projects_no_assignee']
    
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



#------------------------------------------
# Utility Functions - todo separate into separte module

def get_num_unique_pjs_by_window(df, start_date, end_date):
    df_out = wa.filter_tasks_by_time_window(df, start_date, end_date)['eso_project_name']
    pj_count = df_out.nunique()
    return pj_count

def get_list_unique_pjs_by_window(df, start_date, end_date):
    df_out = wa.filter_tasks_by_time_window(df, start_date, end_date)['eso_project_name']
    pj_list = df_out.unique().tolist()
    return pj_list

def get_num_active_cxs(df, start_date, end_date):
    df_out = wa.filter_tasks_by_time_window(df, start_date, end_date)['assignee_name']
    person_count = df_out.nunique()
    return person_count

def get_list_active_cxs(df, start_date, end_date):
    df_out = wa.filter_tasks_by_time_window(df, start_date, end_date)['assignee_name']
    person_list = df_out.unique().tolist()
    return person_list

def get_list_available_cx_persons(df, start_date, end_date):
    active_cx = get_list_active_cxs(df, start_date, end_date)
    cx_avail = list(set(list_all_cx_persons) - set(active_cx))
    return cx_avail

def make_personnel_analysis_df(df_all, df_as, df_unas, max_prsn_available):
    """
    Parameters
    ----------
    df_all : pandas dataframe
        all tasks to be used in this analysis
    df_as : pandas dataframe
        tasks that have an assignee
    df_unas : pandas dataframe
        tasks with no assignee (or by filtering to be treated as having to assignee)
    max_prsn_available: int
        maximum number of people capable of acting in role/capacity of interest
        (note: # todo - upgrade max_prsn_available static value with a schedule of people hired/employed over time)
    """
    
    ## Daily Time Reference 
    start = pd.Timestamp('1/1/2022')
    end = pd.Timestamp('1/1/2024')
    tindex = pd.date_range(start, end)
    df_dater2 = pd.DataFrame(tindex, columns=['date'])
    
    analysis_list = [{
        'date' : r['date'],
        'num_total_projects' : get_num_unique_pjs_by_window(df_all, r['date'], r['date']),
        # 'num_total_tasks' : len(df_all.index)
        'num_projects_no_assignee' : get_num_unique_pjs_by_window(df_unas, r['date'], r['date']),
        'num_persons_occupied' : get_num_unique_pjs_by_window(df_as, r['date'], r['date']),                                                                 
    } for _, r in df_dater2.iterrows()]
    
    df = pd.DataFrame(analysis_list)
    
    # todo - upgrade max_prsn_available static value with a schedule of people hired/employed over time
    df['num_persons_available'] = max_prsn_available - df['num_persons_occupied']
    df['num_persons_supply'] = df['num_persons_available'] - df['num_projects_no_assignee']
    
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



def make_usage_analysis_table(df_in, show_graphs=False, save_graphs=False, use_pj_groups=True):
    
    if use_pj_groups:
        df_tasks = make_project_groups(df_in.copy())
        print('using groups')
    else:
        df_tasks = df_in.copy()
    ## Unassigned - Cx
    # get all cx 'unassigned' tasks (denoted by 'Antti Pitkanen' as assignee)
    df_t = df_tasks
    df_t = df_t[df_t['assignee_name']=='Antti Pitkänen']

    ## Reference Cx Personnel
    pdb_list = wa.get_persondb_api_data()
    df_personnel_master = wa.make_persondb_df(pdb_list)
    df_p_all = df_personnel_master.copy()

    cx_all_persons = df_p_all[df_p_all['role'].isin(['Commissioning Manager', 'Commissioning Engineer'])].sort_values('region')
    cx_all_persons_count = cx_all_persons['person_name'].nunique()
    list_all_cx_persons = cx_all_persons['person_name'].tolist()

    ## Assigned - Cx
    # Get tasks assigned to Cx Managers or Cx Engineers
    roles_list = ['Commissioning Manager', 'Commissioning Engineer']
    df_ass = df_tasks.copy()
    df_ass = df_ass[df_ass['role'].isin(roles_list)]

    # exclude unassigned tasks
    df_ass = df_tasks.copy()
    exclude_assignee_list = ['Antti Pitkänen', 'unassigned']
    df_ass = df_ass[~df_ass['assignee_name'].isin(exclude_assignee_list)]

    # verify assignees against master list
    # todo determine if we need to add region filter
    df_ass = df_ass[df_ass['assignee_name'].isin(cx_all_persons['person_name'].to_list())]

    ## Assigned and Unassigned 
    df_cx_all = pd.concat([df_t, df_ass])    
    
    region_options = ['AMER', 'EUAF', 'MEA']
    
    list_reg_collector = list()
    list_task_reg_collector = list()
    for reg in region_options:
        
        # convert to list (provisional to make use of existing code)
        selected_regions = [reg]
        cx_all_persons_count_fil = cx_all_persons[cx_all_persons['region'].isin(selected_regions)]['person_name'].nunique()
            
        df_cx_all_fil = df_cx_all[df_cx_all['region'].isin(selected_regions)]
        df_t_fil = df_t[df_t['region'].isin(selected_regions)]
        df_ass_fil = df_ass[df_ass['region'].isin(selected_regions)]
        
        #-------Troubleshooting------------
#         print("--Selected Region--")
#         print(selected_regions)
#         print("--People Count--")
#         display(cx_all_persons_count)
        
#         print("--Personnel List--")
#         display(df_cx_all_fil)
        
#         print("--Not Assigned--")
#         display(df_t_fil)
        
#         print("--Assigned--")
#         display(df_ass_fil)
        #-----------------------------------

        # get analysis values
        df_cx_combo_fil = make_personnel_analysis_df(df_cx_all_fil, df_t_fil, df_ass_fil, cx_all_persons_count_fil)
        # Add region column
        df_cx_combo_fil['region'] = reg
        list_reg_collector.append(df_cx_combo_fil)
        
        temp_df_tim = df_cx_all_fil.replace('Antti Pitkänen','not_assiged')
        temp_df_tim['region'] = reg
        list_task_reg_collector.append(temp_df_tim)
        
        # Dash output prep
        df_persons_reference = cx_all_persons[cx_all_persons['region'].isin(selected_regions)]
        #display(df_persons_reference)
        
        
        if show_graphs or save_graphs:
            # output prep
            df_persons_reference = cx_all_persons[cx_all_persons['region'].isin(selected_regions)]
            table_persons_ref = df_persons_reference.to_dict('records')
            
            fig_daily_usage = px.area(df_cx_combo_fil, x='date', y='num_persons_supply', 
               title=f'Combined Commissioning Manager and Engineer Supply over Time \n {selected_regions} <br>(people available) - (people needed)',
                labels={'num_persons_supply': 'Available Supply (number of people)'})
            fig_daily_usage.update_traces(line_color='#00AA00')

            temp_df_tim = df_cx_all_fil.replace('Antti Pitkänen','not_assiged')
            fig_cx_reg_gantt = px.timeline(temp_df_tim.sort_values('start_date'), x_start="start_date", x_end="due_date", y="eso_project_name", color='assignee_name')
            fig_cx_reg_gantt.update_yaxes(autorange="reversed") # otherwise tasks are listed from the bottom up
            fig_cx_reg_gantt.update_xaxes(
                dtick="M1",
                tickformat="%b\n%Y")
            #fig_cx_reg_gantt.update_yaxes(ticklabelposition="inside")
            fig_cx_reg_gantt.update_xaxes(range=['2022-01-01', '2024-01-01'])

            fig_total_pjs_reg = px.line(df_cx_combo_fil, x='date', y='num_total_projects', title=f'Total Projects - Region: {selected_regions}',
                labels={'num_total_projects': 'Num Total Projects', 'date': 'Date'})
            fig_total_pjs_reg.update_xaxes(
                dtick="M1",
                tickformat="%b\n%Y")
            
            fig_overlay = px.line(df_cx_combo_fil, x='date', y='num_total_projects', title=f'Total Projects - Region: {selected_regions}',
                labels={'num_total_projects': 'Num Total Projects', 'date': 'Date'},
                width=1000, height=500)
            x_bar = df_cx_combo_fil['date'].tolist()
            y_bar = df_cx_combo_fil['num_persons_supply'].tolist()
            fig_overlay.add_bar(x=x_bar, y=y_bar, name="Personnel Supply")
            fig_overlay.update_yaxes(dtick=1)
            
            
            if save_graphs:
                fig_daily_usage.write_image(f"daily_usage_{reg}.jpeg", width=1200, height=500, scale=1)
                fig_cx_reg_gantt.write_image(f"assignee_gant_{reg}.jpeg", width=1500, height=500, scale=1)
                fig_total_pjs_reg.write_image(f"total_projects_{reg}.jpeg", width=900, height=500, scale=1)
                
            if show_graphs:
                fig_overlay.show()
                fig_daily_usage.show()
                fig_cx_reg_gantt.show()
                fig_total_pjs_reg.show()
        
    return pd.concat(list_reg_collector), pd.concat(list_task_reg_collector)


# Fetch Asana Data 
rsc_task_list = wa.get_api_rsc_tasks()
df_tasks = wa.make_rsc_tasks_df(rsc_task_list)
df_tasks = wa.prep_task_display_table(df_tasks)


print('Making Usage Table')
df_forbi, df_tk_bi = make_usage_analysis_table(df_tasks.copy(), show_graphs=False, save_graphs=True)


print('Saving Files')
filename_usage = 'resource_forecast.csv'
filename_tasks = 'resource_tasks_forecast.csv'
filename_timestamp = 'data_timestamp.csv'

folder_string = "C:/Users/dha042/Documents/Resourcing_Prototype/staging_python_test/"
filepath_usage = Path(folder_string + filename_usage)
filepath_tasks = Path(folder_string + filename_tasks)
filepath_timestamp = Path(folder_string + filename_timestamp)

filepath_usage.parent.mkdir(parents=True, exist_ok=True)
filepath_tasks.parent.mkdir(parents=True, exist_ok=True)
filepath_tasks.parent.mkdir(parents=True, exist_ok=True)

df_forbi.to_csv(filepath_usage,index=False)
df_tk_bi.to_csv(filepath_tasks,index=False)

# Timestamp
print(datetime.now(timezone.utc).astimezone())

data_timestamp = datetime.now(pytz.timezone('US/Pacific'))
timestamp_date = data_timestamp.date().strftime("%d-%m-%Y")
timestamp_time = data_timestamp.strftime("%H:%M:%S")
timestamp_timezone = 'US/Pacific'

ts_list = [['date',timestamp_date],['time', timestamp_time], ['timezone', timestamp_timezone]]

df_tstamp = pd.DataFrame(ts_list, columns=['name', 'value'])
df_tstamp.to_csv(filepath_timestamp, index=False)

print('Assets Generated. Can be found at: ')
print(f'{folder_string}')