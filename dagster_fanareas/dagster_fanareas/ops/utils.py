import requests
import pandas as pd
import sqlalchemy
import os
from dagster_fanareas.constants import api_key, tm_api_key, tm_host
from itertools import chain
from dagster import op
import time
import re
import datetime
from datetime import datetime

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
def create_db_session():
    uid = os.getenv("POSTGRES_USER")
    pwd = os.getenv("POSTGRES_PWD")
    server = os.getenv("POSTGRES_HOST")
    port = "25060"
    db = os.getenv("TM_POSTGRES_DBNAME")
    url = f"postgresql://{uid}:{pwd}@{server}:{port}/{db}"
    engine = sqlalchemy.create_engine(url)
    return engine

@op
def get_fields(response):
    data = response.json()['data']
    if type(data) == list:
        keys = tuple(data[0].keys())
    else:
        keys = tuple(data.keys())
    return keys

@op
def api_call(url):
    headers = { 'Authorization': api_key }
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response
    else:
        print(f"Error: {response.status_code}")
        return None

@op
def fetch_data(url):
    data = []
    result = api_call(url)
    if 'data' in result.json().keys():
        while True:
            data.append(result.json()['data'])
            # context.log.info('executing data statement')
            url = result.json()['pagination']['next_page']
            limit = result.json()['rate_limit']['remaining']
            if limit == 1:
                seconds_until_reset = result.json()['rate_limit']['resets_in_seconds']
                # context.log.info(seconds_until_reset)
                time.sleep(seconds_until_reset)
                continue
            else:
                has_more = result.json()['pagination']['has_more']
                if has_more == False:
                    # context.log.info('breaking the loop')
                    break
                result = api_call(url)
        result_df = pd.DataFrame(list(chain(*data)))
    else:
        # context.log.info('executing else statement')
        result_df = pd.DataFrame([])
    return result_df

@op
def upsert(new_df: pd.DataFrame, existing_df: pd.DataFrame) -> pd.DataFrame:
    # Perform upsert (merge) based on the 'id' column
    existing_df.set_index('id', inplace=True)
    new_df.set_index('id', inplace=True)
    existing_df.update(new_df, overwrite=True)
    existing_df.reset_index(inplace=True)
    new_df.reset_index(inplace=True)
    df = pd.concat([existing_df, new_df[~new_df['id'].isin(existing_df['id'])]], ignore_index=True)
    return df

@op
def flatten_list(nested_list):
    flattened_list = []
    for item in nested_list:
        if isinstance(item, list):
            flattened_list.extend(flatten_list(item))
        else:
            flattened_list.append(item)
    return flattened_list

@op
def get_dim_name_and_id(dim: str) -> dict:
    engine = create_db_session()
    url = f"https://fanareas.com/api/{dim}/generateId"
    _id = requests.get(url).json()
    query = f"""select name from {dim} where id = {_id}"""
    df = pd.read_sql(query, con=engine)
    name = df['name'].iloc[0]
    return {'name': name, 'id': _id}

@op
def post_json(json_data, url):
    response = requests.post(url, json=json_data)
    if response.status_code == 400:
        return False

    if response.status_code == 200:
        print('POST request successful!')
        print('Response:', response.text)
    else:
        print('POST request failed with status code:', response.status_code)
        print('Response:', response.text)
    
    return True

@op
def call_url(url):
    response = requests.post(url)

    if response.status_code == 200:
        print('POST request successful!')
        print('Response:', response.text)
    else:
        print('POST request failed with status code:', response.status_code)
        print('Response:', response.text)
    
    return True