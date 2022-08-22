# Run this app with `python app.py` and
# visit http://127.0.0.1:8050/ in your web browser.

from dash import Dash, html, dcc, dash_table, html, Input, Output
import dash_auth
import dash_bootstrap_components as dbc
import plotly.express as px
import pandas as pd
import numpy as np
from datetime import date

import wartsila_asana_utils as wa
import asana


VALID_USERNAME_PASSWORD_PAIRS = {
    'wartsila_eso': 'delivery22'
}

# Initiate Dash App
app = Dash(external_stylesheets=[dbc.themes.BOOTSTRAP])
auth = dash_auth.BasicAuth(
    app,
    VALID_USERNAME_PASSWORD_PAIRS
)



#-- UI Options 

region_options = ['AMER', 'EUAF', 'MEA']
role_options = ['System Engineer', 'Solution Engineer', 'CPE Electrical', 'CPE Civil',
                'Commissioning Manager', 'unassigned']


#------------------------------------------
# Asana Data 
rsc_task_list = wa.get_api_rsc_tasks()
df_tasks = wa.make_rsc_tasks_df(rsc_task_list)
df_tasks = wa.prep_task_display_table(df_tasks)



#Preliminary Data Processing

## Unassigned - Cx
# get all cx 'unassigned' tasks (denoted by 'Antti Pitkanen' as assignee)
df_t = df_tasks.copy()
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

# compare assignees against master list
df_ass = df_ass[df_ass['assignee_name'].isin(cx_all_persons['person_name'].to_list())]

## Assigned and Unassigned 
df_cx_all = pd.concat([df_t, df_ass])


## Time Reference 
start = pd.Timestamp('1/1/2022')
end = pd.Timestamp('1/1/2024')
tindex = pd.date_range(start, end)
df_dater2 = pd.DataFrame(tindex, columns=['date'])

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

#------------------------------------------
# CX Analysis 
df_cx_combo = make_personnel_analysis_df(df_cx_all, df_t, df_ass, cx_all_persons_count)

#------------------------------------------
# Layout
app.layout = dbc.Container(html.Div(children=[
    html.H1(children='Wartsila ESO', style={'marginTop': 10}),
    html.H3(children='Resource Utilization'),
    html.Hr(),

    #-- Input
    dbc.Row(
        dbc.Col(            
            html.Div(dcc.Dropdown(
                options=[{'label': i, 'value': i} for i in region_options], 
                value=region_options,
                multi=True,
                placeholder='select a region',
                id='region-select')
            ),
        width=4, style={'marginTop': 0})
    ),

    # dbc.Row(
    #     dbc.Col(
    #         html.Div(dcc.Dropdown(
    #             options=role_options, 
    #             value='System Engineer',
    #             placeholder='select a role',
    #             id='role-select')
    #         ), 
    #     width=4, style={'marginTop': 10})
    # ),

    # html.Div([
    #     dcc.DatePickerRange(
    #         id='date-picker-range',
    #         min_date_allowed=date(2022, 1, 1),
    #         max_date_allowed=date(2025, 12, 31),
    #         initial_visible_month=date(2022, 8, 12),
    #         start_date=date(2022, 8, 1),
    #         end_date=date(2022, 9, 1)
    #     )],
    #     style={'marginTop': 10},
    # ),
    html.Hr(),

    #-- Output Display 
    html.Div([
        dash_table.DataTable(
            #df_tasks.to_dict('records'),
            [{"name": i.replace('_', ' '), "id": i} for i in cx_all_persons.columns],
            filter_action='custom',
            sort_action='native',
            style_table={'height': '300px', 'overflowY': 'auto'},
            id='cx-personnel-table'
        )], 
        style={'marginTop': 25},
    ),
    
    html.Hr(),
    html.H2(children='Visual of Role Usage'), 
    dbc.Row([
        dbc.Col(
            html.Div([
                dcc.Graph(
                    id='cx-daily-supply-reg-fig',
                )
            ]),
        width = 12
        ), 
    ]),

    dbc.Row([
        dbc.Col(
            html.Div([
                dcc.Graph(
                    id='cx-total-pjs-reg-fig',
                )
            ]),
        width = 12
        ), 
    ]),


    dbc.Row([
        dbc.Col(
            html.Div([
                dcc.Graph(
                    id='cx-gantt-reg-fig',
                )
            ]),
        width = 12
        ), 
    ]),

    # html.Hr(),
    # html.H2(children="Number of People Occuppied by Role"),
    # dbc.Row([
    #     dbc.Col(
    #         html.Div([
    #             dcc.Graph(
    #                 id='fig-ppl-role-bar',
    #             )
    #         ]),
    #     width = 9
    #     ), 
    #     dbc.Col(
    #         html.Div([
    #             dash_table.DataTable(
    #                 columns=[{"name": 'role', "id": 'role'},
    #                         {"name": 'role count', "id": 'role_count'}],
    #                 id='table-ppl-role-cnt'
    #             )
    #         ]), style={'marginTop': 25}, 
    #     width = 3    
    #     )
    # ]),

    html.Hr()
]))

#---------------------------------------------
# Callbacks 

# @app.callback(
#     Output('tasks-table', 'data'),
#     Input('role-select', 'value'),
#     Input('region-select', 'value'), 
#     Input('date-picker-range', 'start_date'),
#     Input('date-picker-range', 'end_date')
# )

# def update_tasks_table(sel_roles, sel_regions, dt_start, dt_end):
#     df_out = df_tasks

#     # filtering data
#     if (dt_start is not None) and (dt_end is not None):
#         start_date = np.datetime64(date.fromisoformat(dt_start))
#         end_date = np.datetime64(date.fromisoformat(dt_end))
#         df_out = wa.filter_tasks_by_time_window(df_tasks, start_date, end_date)

#     if sel_roles is not None:
#         df_out = wa.filter_tasks_by_role(df_out, [sel_roles])

#     if sel_regions is not None: 
#         df_out = wa.filter_tasks_by_region(df_out, [sel_regions])

#     # display raw data table with only desired columns
#     df_table = wa.prep_task_display_table(df_out)
#     df_table = wa.format_dates_as_strings(df_table).to_dict('records')

#     return df_table

# @app.callback(
#     Output('table-ppl-role-cnt', 'data'),
#     Output('fig-ppl-role-bar', 'figure'),
#     Input('region-select', 'value'), 
#     Input('date-picker-range', 'start_date'),
#     Input('date-picker-range', 'end_date')
# )
# def update_role_usage_analysis(sel_regions, dt_start, dt_end):
#     df_out = df_tasks

#     # filtering data
#     if (dt_start is not None) and (dt_end is not None):
#         start_date = np.datetime64(date.fromisoformat(dt_start))
#         end_date = np.datetime64(date.fromisoformat(dt_end))
#         df_out = wa.filter_tasks_by_time_window(df_out, start_date, end_date)

#     if sel_regions is not None: 
#         df_out = wa.filter_tasks_by_region(df_out, [sel_regions])

#     # count number of unique people occupied at a time
#     df_unq_ppl = wa.make_unique_persons_per_role_counts_df(df_out)
#     df_unq_ppl = df_unq_ppl.to_dict('records')

#     # bar chart of unique people occupied per role at a time
#     fig_unq_ppl_bar = wa.make_unique_persons_per_role_counts_barchart(df_out)

#     return df_unq_ppl, fig_unq_ppl_bar

@app.callback(
    Output('cx-personnel-table', 'data'),
    Output('cx-daily-supply-reg-fig', 'figure'),
    Output('cx-gantt-reg-fig', 'figure'),
    Output('cx-total-pjs-reg-fig', 'figure'),
    Input('region-select', 'value'),
    )
def update_cx_regional_analysis(selected_regions):

    # apply selections as data filters
    cx_all_persons_count_fil = cx_all_persons[cx_all_persons['region'].isin(selected_regions)]['person_name'].nunique()
    df_cx_all_fil = df_cx_all[df_cx_all['region'].isin(selected_regions)]
    df_t_fil = df_t[df_t['region'].isin(selected_regions)]
    df_ass_fil = df_ass[df_ass['region'].isin(selected_regions)]

    # get analysis values
    df_cx_combo_fil = make_personnel_analysis_df(df_cx_all_fil, df_t_fil, df_ass_fil, cx_all_persons_count_fil)

    # output prep
    df_persons_reference = cx_all_persons[cx_all_persons['region'].isin(selected_regions)]
    table_persons_ref = df_persons_reference.to_dict('records')

    fig_daily_usage = px.bar(df_cx_combo_fil, x='date', y='num_persons_supply', 
       title=f'Combined Commissioning Manager and Engineer Supply over Time \n {selected_regions} <br>(people available) - (people needed)',
        labels={'num_persons_supply': 'Available Supply (number of people)'})

    temp_df_tim = df_cx_all_fil.replace('Antti Pitkänen','not_assiged')
    fig_cx_reg_gantt = px.timeline(temp_df_tim.sort_values('start_date'), x_start="start_date", x_end="due_date", y="eso_project_name", color='assignee_name')
    fig_cx_reg_gantt.update_yaxes(autorange="reversed") # otherwise tasks are listed from the bottom up
    fig_cx_reg_gantt.update_xaxes(
        dtick="M1",
        tickformat="%b\n%Y")
    fig_cx_reg_gantt.update_xaxes(range=['2022-01-01', '2024-01-01'])

    fig_total_pjs_reg = px.line(df_cx_combo_fil, x='date', y='num_total_projects', title=f'Total Projects - Regions: {selected_regions}',
        labels={'num_total_projects': 'Num Total Projects', 'date': 'Date'})
    fig_total_pjs_reg.update_xaxes(
        dtick="M1",
        tickformat="%b\n%Y")

    return table_persons_ref, fig_daily_usage, fig_cx_reg_gantt, fig_total_pjs_reg

#---------------------------------------------
# Run App 

if __name__ == '__main__':
    app.run_server(debug=True)