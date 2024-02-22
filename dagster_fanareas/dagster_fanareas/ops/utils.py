import requests
import pandas as pd
# import os

from itertools import chain
from dagster import op, OpExecutionContext
import time

@op
def get_records(response):
    records=[]
    data = response.json()['data']
    if type(data) == list:
        for i in range(len(data)):
            records.append(tuple(data[i].values()))
    else:
        records.append(tuple(data.values()))
    return records

@op
def get_fields(response):
    data = response.json()['data']
    if type(data) == list:
        keys = tuple(data[0].keys())
    else:
        keys = tuple(data.keys())
    return keys

@op
def api_call(url, api_key):
    headers = { 'Authorization': api_key }
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response
    else:
        print(f"Error: {response.status_code}")
        return None
    
@op
def fetch_data(context: OpExecutionContext, url, api_key):
    data = []
    result = api_call(url, api_key)
    context.log.info(result)
    context.log.info(result.json())
    if 'data' in result.json().keys():
        while True:
            data.append(result.json()['data'])
            context.log.info('executing data statement')
            url = result.json()['pagination']['next_page']
            limit = result.json()['rate_limit']['remaining']
            if limit == 1:
                seconds_until_reset = result.json()['rate_limit']['resets_in_seconds']
                context.log.info(seconds_until_reset)
                time.sleep(seconds_until_reset)
                continue
            else:
                has_more = result.json()['pagination']['has_more']
                if has_more == False:
                    context.log.info('breaking the loop')
                    break
                result = api_call(url, api_key)
        result_df = pd.DataFrame(list(chain(*data)))
    else:
        context.log.info('executing else statement')
        result_df = pd.DataFrame([])
    return result_df

@op
def upsert(existing_df: pd.DataFrame, new_df: pd.DataFrame) -> pd.DataFrame:
    # Perform upsert (merge) based on the 'id' column
    merged_df = pd.concat([existing_df, new_df], ignore_index=True)
    merged_df = merged_df.drop_duplicates(subset='id', keep='last')
    return merged_df

@op
def flatten_list(nested_list):
    flattened_list = []
    for item in nested_list:
        if isinstance(item, list):
            flattened_list.extend(flatten_list(item))
        else:
            flattened_list.append(item)
    return flattened_list
