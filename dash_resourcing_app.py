# Run this app with `python app.py` and
# visit http://127.0.0.1:8050/ in your web browser.

from dash import Dash, html, dcc, dash_table, html, Input, Output
import dash_bootstrap_components as dbc
import plotly.express as px
import pandas as pd
import numpy as np
from datetime import date

import wartsila_asana_utils as wa


# Initiate Dash App
app = Dash(external_stylesheets=[dbc.themes.BOOTSTRAP])

#-- UI Options 

region_options = ['AMER', 'AFEU', 'MEA']
role_options = ['System Engineer', 'Solution Engineer', 'CPE Electrical', 'CPE Civil',
                'Commissioning Manager', 'unassigned']


#------------------------------------------
# Asana Data 
rsc_task_list = wa.get_api_rsc_tasks()
df_tasks = wa.make_rsc_tasks_df(rsc_task_list)
df_tasks = wa.prep_task_display_table(df_tasks)

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

    dbc.Row(
        dbc.Col(
            html.Div(dcc.Dropdown(
                options=role_options, 
                value='System Engineer',
                placeholder='select a role',
                id='role-select')
            ), 
        width=4, style={'marginTop': 10})
    ),

    html.Div([
        dcc.DatePickerRange(
            id='date-picker-range',
            min_date_allowed=date(2022, 1, 1),
            max_date_allowed=date(2025, 12, 31),
            initial_visible_month=date(2022, 8, 12),
            start_date=date(2022, 8, 1),
            end_date=date(2022, 9, 1)
        )],
        style={'marginTop': 10},
    ),
    html.Hr(),

    #-- Output Display 
    html.Div([
        dash_table.DataTable(
            df_tasks.to_dict('records'),
            [{"name": i.replace('_', ' '), "id": i} for i in df_tasks.columns],
            filter_action='custom',
            sort_action='native',
            style_table={'height': '300px', 'overflowY': 'auto'},
            id='tasks-table'
        )], 
        style={'marginTop': 25},
    ),
    
    html.Hr(),
    html.H2(children="Number of People Occuppied by Role"),
    dbc.Row([
        dbc.Col(
            html.Div([
                dcc.Graph(
                    id='fig-ppl-role-bar',
                )
            ]),
        width = 9
        ), 
        dbc.Col(
            html.Div([
                dash_table.DataTable(
                    columns=[{"name": 'role', "id": 'role'},
                            {"name": 'role count', "id": 'role_count'}],
                    id='table-ppl-role-cnt'
                )
            ]), style={'marginTop': 25}, 
        width = 3    
        )
    ]),

    html.Hr()
]))

#---------------------------------------------
# Callbacks 

@app.callback(
    Output('tasks-table', 'data'),
    Input('role-select', 'value'),
    Input('region-select', 'value'), 
    Input('date-picker-range', 'start_date'),
    Input('date-picker-range', 'end_date')
)

def update_tasks_table(sel_roles, sel_regions, dt_start, dt_end):
    df_out = df_tasks

    # filtering data
    if (dt_start is not None) and (dt_end is not None):
        start_date = np.datetime64(date.fromisoformat(dt_start))
        end_date = np.datetime64(date.fromisoformat(dt_end))
        df_out = wa.filter_tasks_by_time_window(df_tasks, start_date, end_date)

    if sel_roles is not None:
        df_out = wa.filter_tasks_by_role(df_out, [sel_roles])

    if sel_regions is not None: 
        df_out = wa.filter_tasks_by_region(df_out, [sel_regions])

    # display raw data table with only desired columns
    df_table = wa.prep_task_display_table(df_out)
    df_table = wa.format_dates_as_strings(df_table).to_dict('records')

    return df_table

@app.callback(
    Output('table-ppl-role-cnt', 'data'),
    Output('fig-ppl-role-bar', 'figure'),
    Input('region-select', 'value'), 
    Input('date-picker-range', 'start_date'),
    Input('date-picker-range', 'end_date')
)
def update_role_usage_analysis(sel_regions, dt_start, dt_end):
    df_out = df_tasks

    # filtering data
    if (dt_start is not None) and (dt_end is not None):
        start_date = np.datetime64(date.fromisoformat(dt_start))
        end_date = np.datetime64(date.fromisoformat(dt_end))
        df_out = wa.filter_tasks_by_time_window(df_out, start_date, end_date)

    if sel_regions is not None: 
        df_out = wa.filter_tasks_by_region(df_out, [sel_regions])

    # count number of unique people occupied at a time
    df_unq_ppl = wa.make_unique_persons_per_role_counts_df(df_out)
    df_unq_ppl = df_unq_ppl.to_dict('records')

    # bar chart of unique people occupied per role at a time
    fig_unq_ppl_bar = wa.make_unique_persons_per_role_counts_barchart(df_out)

    return df_unq_ppl, fig_unq_ppl_bar

#---------------------------------------------
# Run App 

if __name__ == '__main__':
    app.run_server(debug=True)
