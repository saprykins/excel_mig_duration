import pandas as pd

a = pd.read_csv('./result/task_tcs_durations.csv')
# print(a)

# generate pd w/ start times
# it keeps all the data
df_start_tasks = a.loc[a['Task'].isin([
    '4.1 MGN checks',
    '4.3 Configure replication',
    '4.4 Configure Launch settings'
])]
# print('tasks')
# print(df_start_tasks)

# generate pd w/ end times
df_end_tasks = a.loc[a['Task'].isin([
    '5.2 Cutover (MGN zone)',
    '5.2 Cutover'
])]




# Get app - start time
# Tasks that are considered as start
# Time conversion
df_start_tasks['Start time'] = pd.to_datetime(df_start_tasks['Start time'])
df_start_tasks['End time'] = pd.to_datetime(df_start_tasks['End time'])

# starters = 
# print('start time')
# print(df_start_tasks)

# For each task, find the min between start and end 
# Is used because often the Start time isn't available
df_start_tasks['Min start found'] = df_start_tasks[['Start time', 'End time']].min(axis=1)
# print('start')
# print(df_start_tasks)

df_app_start = df_start_tasks.groupby('App name')['Min start found'].min().rename('App start')
# print('start a column')
# print(df_app_start)

# check only start time of start tasks
# df_app_start = df_start_tasks.groupby('App name')['Start time'].min().rename('App start')
# print('start')
# print(df_start_tasks)




# Get app - end time
# Transform to dates
df_end_tasks['End time'] = pd.to_datetime(df_end_tasks['End time'])

# 



df_app_end = df_end_tasks.groupby('App name')['End time'].max().rename('App end')


# print('end')
# print(df_app_end)


# join
merged_pd = pd.merge(df_app_start, df_app_end, on='App name')



# duration
merged_pd['Duration'] = round((merged_pd['App end'] - merged_pd['App start']).dt.total_seconds()/60)
# print(merged_pd)
merged_pd.to_csv('./result/tcs_app_durations.csv')




'''




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
merged_pd.to_csv('./result/app_durations_110723.csv')
'''
