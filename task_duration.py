import requests
import json
import base64
import pandas as pd
from datetime import datetime
import time
# import openpyxl

ORGANIZATION_NAME = "go**"
PROJECT_NAME = "Mi**"
pat = "rv***"
authorization = str(base64.b64encode(bytes(':'+pat, 'ascii')), 'ascii')


cols_duration =  [
    "App id", 
    "App name", 
    "Feature id", 
    "Feature", 
    "User story id", 
    "User story", 
    "Task id", 
    "Task", 
    "Start time",
    "End time",
    "Duration (min)"
]

df_duration = pd.DataFrame([], columns = cols_duration)



def get_childs_list(app_id):
    #
    # Can be used to get all types of childs (user stories, features ...)
    #

    url = f"https://dev.azure.com/{ORGANIZATION_NAME}/{PROJECT_NAME}/_apis/wit/workitems/{app_id}/?$expand=all"
    headers = {
        'Accept': 'application/json',
        'Authorization': 'Basic '+ authorization,
    }
    response = requests.get(url, headers=headers)
    response_json = json.loads(response.text)
    
    user_story_work_items = response_json["relations"]

    child_ids = []
    for relation in user_story_work_items:
        if relation["rel"] == "System.LinkTypes.Hierarchy-Forward":
            url = relation["url"]
            parts = url.split('/')
            id = parts[-1]
            child_ids.append(id)
    childs_work_item_ids = child_ids
    # print(f"User story ids: {user_story_work_item_ids}")
    return childs_work_item_ids





def get_duration(workitem_id):
    #
    # 
    #

    url = 'https://dev.azure.com/' + ORGANIZATION_NAME + '/' + PROJECT_NAME + '/_apis/wit/workItems/' + str(workitem_id) + '/updates?api-version=7.0'

    headers = {
        'Accept': 'application/json',
        'Authorization': 'Basic '+ authorization
    }
    response = requests.get(
        url = url,
        headers=headers,
    )
    changes = response.json()["value"]

    for state_change in reversed(changes):
        try:
            title = state_change["fields"]["System.Title"]['newValue'] 
            break
        except:
            title = None
    
    
    app_history = response.json()["value"]

    status_change = None

    for state_change in reversed(app_history):
        try:
            if state_change["fields"]["System.State"]['oldValue'] == "In-Progress" and state_change["fields"]["System.State"]['newValue'] == "Closed":
                status_change = state_change
                break
        except:
            val = 0

    # Calculate the duration of the work item in progress
    if status_change is not None:
        try:
            start_time = status_change["fields"]['Microsoft.VSTS.Common.StateChangeDate']['oldValue']
            end_time = status_change["fields"]['Microsoft.VSTS.Common.StateChangeDate']['newValue']
            
            in_progress_date = datetime.strptime(status_change["fields"]['Microsoft.VSTS.Common.StateChangeDate']['oldValue'], '%Y-%m-%dT%H:%M:%S.%fZ')
            closed_date = datetime.strptime(status_change["fields"]['Microsoft.VSTS.Common.StateChangeDate']['newValue'], '%Y-%m-%dT%H:%M:%S.%fZ')

            duration = closed_date - in_progress_date

            duration_sec = duration.seconds
            duration_min = duration_sec/60

        except:
            duration_min = None
            start_time = None
            end_time = None
    else:
        duration_min = None
        start_time = None
        end_time = None

    if duration_min is not None:
        duration_min = round(duration_min)

    #
    # If closed without in progress
    #

    for state_change in reversed(app_history):
        try:
            if state_change["fields"]["System.State"]['oldValue'] == "New" and state_change["fields"]["System.State"]['newValue'] == "Closed":
                status_change = state_change
                break
        except:
            val = 0

    # Calculate the duration of the work item in progress
    if status_change is not None:
        try:
            # start_time = status_change["fields"]['Microsoft.VSTS.Common.StateChangeDate']['oldValue']
            end_time = status_change["fields"]['Microsoft.VSTS.Common.StateChangeDate']['newValue']
            closed_date = datetime.strptime(status_change["fields"]['Microsoft.VSTS.Common.StateChangeDate']['newValue'], '%Y-%m-%dT%H:%M:%S.%fZ')
        except:
            duration_min = None
            start_time = None
            end_time = None
    # end of closed w/o in progress


    return (duration_min, title, start_time, end_time)





def save_duration_to_df(app_id, df_duration):
    #
    #
    #
    feature_ids = get_childs_list(app_id)
    # print(feature_ids)
    duration_x, app_title, start_time, end_time = get_duration(app_id)
    for feature_id in feature_ids:
        user_story_ids = get_childs_list(feature_id)
        duration_y, feature_title, start_time, end_time = get_duration(feature_id)
        # print(user_story_ids)
        for user_story_id in user_story_ids:
            task_ids = get_childs_list(user_story_id)
            duration_z, user_story_title, start_time, end_time = get_duration(user_story_id)
            # print(task_ids)
            for task_id in task_ids:
                duration, task_title, start_time, end_time = get_duration(task_id)
                new_row = [app_id, app_title, feature_id, feature_title, user_story_id, user_story_title, task_id, task_title, start_time, end_time, duration]
                new_df = pd.DataFrame([new_row], columns=cols_duration)
                df_duration = pd.concat([df_duration, new_df], ignore_index = True)  
    return df_duration



def get_list_of_migrated_apps():
    #
    #
    #
    list_of_apps = []
    # url = 'https://dev.azure.com/' + ORGANIZATION_NAME + '/' + PROJECT_NAME + '/_apis/wit/workItems/' + str(workitem_id) + '/updates?api-version=7.0'
    
    
    # good query (not yet to start, changed during the last 7 days)
    url = "https://dev.azure.com/" + ORGANIZATION_NAME + "/" + PROJECT_NAME + "/_apis/wit/wiql/8fa6a38c-1b54-425c-93d2-4fd360c11f84"
    
    # for tests
    # url = "https://dev.azure.com/" + ORGANIZATION_NAME + "/" + PROJECT_NAME + "/_apis/wit/wiql/cf2b3520-cd93-433d-8c45-46ab4c4c9ada"
    headers = {
        'Accept': 'application/json',
        'Authorization': 'Basic '+ authorization
    }
    response = requests.get(
        url = url,
        headers=headers,
    )
    apps_raw_data = response.json()["workItems"]
    for app in apps_raw_data:
        list_of_apps.append(app["id"])
    return list_of_apps

# print(get_list_of_migrated_apps())
    


# 
# MAIN
#
start_time = time.time()/60 # sec



# df_duration = save_duration_to_df(app_id, df_duration)


"""
app_id = "173026"
df_duration = save_duration_to_df(app_id, df_duration)
print(df_duration)

"""
list_of_apps = get_list_of_migrated_apps()
print(len(list_of_apps))
# list_of_apps = [264785]
# list_of_apps = list_of_apps[50:100]


for app_id in list_of_apps:
    df_duration = save_duration_to_df(app_id, df_duration)

# print(df_duration)
# display(df_duration)

# df_duration.to_excel('./results/ADO_MS_duration_extract_3.xlsx', sheet_name='duration', index=False)
df_duration.to_csv('./results/ADO_MS_duration_extract_17-270623.csv', index = False)
end_time = time.time()/60 # sec
print(end_time - start_time)
