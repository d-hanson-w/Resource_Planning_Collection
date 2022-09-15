import wartsila_asana_utils as wa
# breaking down analysis function 

#1. get unassigned tasks 
#2. get assigned tasks 

#3. function option - only return tasks matching people in list of job title
#3a. get list of people by job title
#3b. modify assigned tasks to only those assigned to someone in the list of people with that title

# returns
#------------
# df_assigned
# df_unassigned
# df_all (determine if it makes more sense to do this later)

# note: move project groups to data engineering section

#-------------------------------------------------------------
# Analysis Function(s) 
#-------------------------------------------------------------

def calculate_aggregate_workload_analysis(df_assigned, df_unassigned, total_personnel_count, start_date='1/1/2022', end_date='1/1/2024', workload_column='workload_units'):
    """
    Input
    -----
    df_assigned : pandas dataframe - assigned tasks
    df_unassigned : pandas dataframe - unassigned tasks
    total_personnel_count : number of people employed at wartsila in relevant roles
    start_date : string - date to begin analysis (e.g. '1/1/2022')
    end_date : string - date to end analysis (e.g. '1/1/2022')
    
    Returns
    -------
    df_workload_analysis : pandas dataframe - dataframe with workload analysis values
    """
    
    ## Daily Time Reference 
    start = pd.Timestamp(start_date)
    end = pd.Timestamp(end_date)
    tindex = pd.date_range(start, end)
    df_dater2 = pd.DataFrame(tindex, columns=['date'])
    
    ## Calculate Workload Columns for each Day
    analysis_list = list()
    for _, r in df_dater2.iterrows():
        df_ass_day = wa.filter_tasks_by_time_window(df_ass, r['date'], r['date'])
        df_unass_day = wa.filter_tasks_by_time_window(df_unass, r['date'], r['date'])
        
        # Calculations
        # workload-based
        ass_workload_val = df_unass_day[workload_column].sum(axis=0)
        unass_workload_val = df_ass[workload_column].sum(axis=0)        
        
        # calculate cumulative workload values and append to collector list
        analysis_list.append(
            {
            'date' : r['date'],
            'workload_unassigned': ass_workload_val,
            'workload_assigned': unass_workload_val,
            'total_workload': ass_workload_val + unass_workload_val,
            #'workload_supply': una,
            }
        )
    
    df_workload_analysis = pd.DataFrame(analysis_list)        
            
    # potential future workload columns
    # 'epc_total_workload'
    # 'eeq_total_workload'
    # 'epc_assigned_workload'
    # 'epc_unassigned_workload'
    # 'eeq_assigned_workload'
    # 'eeq_unassigned_workload'
            
    return df_workload_analysis

            
            
def calculate_personnel_workload_analysis(df_personnel):
            
# columns 
# -name
            # -date
            # -workload_units_assigned
            # -workload_units_unassigned
            # -epc_workload_units_assigned
            # -eeq_workload_units_assigned
            
            # get each person name from the personnel database
            df_personnel = wa.get_personnel_project_data()
            
            # filter task list by the name 
            # perform workload analysis for each day to get above columns
            # 
#-------------------------------------------------------------
# Graphing Functions
#-------------------------------------------------------------

