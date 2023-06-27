import pandas as pd

a = pd.read_csv('./results/input_for_pandas.csv')

# generate pd w/ start times
df_start_tasks = a.loc[a['Task'].isin([
    'TA50.10.040 - Stop scheduled application specific jobs - on source VM', 
    'TA50.10.050 - Stop application specific services on VMs in PIaaS', 
    'TA50.20.020 - Shutdown On-Premises Source Servers using AXALIS Tool'
])]



# generate pd w/ end times
df_end_tasks = a.loc[a['Task'].isin([
    'TA50.40.070 - Verify Orchestration tasks executed successfully',
    'TA50.40.150 - Restart the servers in a specific order if applicable',
    'TA50.40.160 - [CHECK] Azure monitoring agents installed',
    'TA50.40.170 - [CHECK] Azure Backup enabled'
])]



# Get app - start time
# Transform to dates
df_start_tasks['Start time'] = pd.to_datetime(df_start_tasks['Start time'])
df_app_start = df_start_tasks.groupby('App name')['Start time'].min().rename('App start')


# Get app - end time
# Transform to dates
df_end_tasks['End time'] = pd.to_datetime(df_end_tasks['End time'])
# Group by apps, get max value 
df_app_end = df_end_tasks.groupby('App name')['End time'].max().rename('App end')


# join
merged_pd = pd.merge(df_app_start, df_app_end, on='App name')



# duration
merged_pd['Duration'] = round((merged_pd['App end'] - merged_pd['App start']).dt.total_seconds()/60)

# print(merged_pd.head())
merged_pd.to_csv('output_x.csv')
