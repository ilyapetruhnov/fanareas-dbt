import requests
import pandas as pd
import sqlalchemy
import os
from dagster_fanareas.constants import tm_api_key, tm_host, tm_url
from itertools import chain
from dagster import op
import time
import re
import datetime
from datetime import datetime

@op
def rename_camel_col(camel_str):
    # Identify the uppercase letters and replace them with an underscore followed by the lowercase letter
    snake_str = re.sub(r'([A-Z])', r'_\1', camel_str).lower()
    # Remove any leading underscore if it exists
    if snake_str.startswith('_'):
        snake_str = snake_str[1:]
    return snake_str

@op
def tm_api_call(url, params):
    headers = {"X-RapidAPI-Key": tm_api_key, "X-RapidAPI-Host": tm_host}
    response = requests.get(url, headers=headers, params = params)
    if response.status_code == 200:
        return response
    else:
        print(f"Error: {response.status_code}")
        return None

@op
def tm_fetch_data(url, params, key=None):
    data = []
    result = tm_api_call(url, params)
    if key is not None:
        data.append(result.json()['data'].get(key))
    else:
        data.append(result.json()['data'])
    result_df = pd.DataFrame(list(chain(*data)))
    return result_df

@op
def tm_fetch_player_performance(season_id, player_id):
    url = "https://transfermarkt-db.p.rapidapi.com/v1/players/performance-details"
    params = {"competition_id":"GB1","season_id":season_id,"player_id":player_id,"locale":"US"}
    frames = []
    response = tm_api_call(url, params)
    data = response.json()['data']
    for i in range(len(data)):
        match = data[i]
        match_id = match['match']['id']
        performance = data[i]['performance']
        performance['id'] = match_id
        performance['player_id'] = player_id
        performance['season_id'] = season_id
        df = pd.DataFrame.from_dict(performance, orient='index').T
        frames.append(df)
    result_df = pd.concat(frames)
    result_df = result_df.fillna(0)
    cols = ['id','player_id', 'season_id', 'goals', 'assists', 'ownGoals', 'yellowCardMinute',
        'yellowRedCardMinute', 'redCardMinute', 'minutesPlayed',
        'substitutedOn', 'substitutedOff']
    for col in cols:
        result_df[col] = result_df[col].astype(int)
    cols.append('position')
    result_df = result_df[cols]
    return result_df

@op
def tm_fetch_squads(season_id, team_id):
    url = "https://transfermarkt-db.p.rapidapi.com/v1/clubs/squad"
    frames = []
    params = {"season_id":season_id,"locale":"US","club_id":team_id}
    response = tm_api_call(url, params)
    try:
        data = response.json()['data']
        for i in data:
            sliced_dict = {k: i[k] for k in list(i.keys())}
            df = pd.DataFrame.from_dict(sliced_dict, orient='index').T
            df['team_id'] = team_id
            df['season_id'] = season_id
            df['joined'] = df['joined'].apply(lambda x: datetime.fromtimestamp(x))
            df['contract_until'] = df['contractUntil'].apply(lambda x: datetime.fromtimestamp(x))
            df['market_value'] = df['marketValue'].apply(lambda x: x['value'])
            df['market_value'] = df['marketValue'].apply(lambda x: x['value'])
            df['market_value_currency'] = df['marketValue'].apply(lambda x: x['currency'])
            df['market_value_progression'] = df['marketValue'].apply(lambda x: x['progression'])
            cols = ['player_id','team_id','season_id','name','joined', 'contract_until',
                    'captain', 
                    'isLoan', 
                    'wasLoan',   
                    'shirtNumber', 
                    'age',
                    'market_value', 
                    'market_value_currency',
                    'market_value_progression']
            df = df.rename(columns={'id': 'player_id'})
            frames.append(df[cols])

        result_df = pd.concat(frames)
    except Exception as e:
        result_df = pd.DataFrame([])
    return result_df

def match_result(goalsHome, homeTeamID, goalsAway, awayTeamID, result_type):
    if goalsHome == goalsAway:
        return 0
    elif goalsHome > goalsAway and result_type == 'win':
        return homeTeamID
    elif goalsHome < goalsAway and result_type == 'win':
        return awayTeamID
    elif goalsHome < goalsAway and result_type == 'lose':
        return homeTeamID
    elif goalsHome > goalsAway and result_type == 'lose':
        return awayTeamID
    else:
        return None
    
@op
def tm_fetch_match_result(match_id):
    url = "https://transfermarkt-db.p.rapidapi.com/v1/fixtures/result"

    params = {"locale":"US","fixture_id":match_id}
    response = tm_api_call(url, params)
    data = response.json()['data']
    result = pd.DataFrame.from_dict(data, orient='index').T
    return result

@op
def tm_fetch_match_stats(match_id):
    url = "https://transfermarkt-db.p.rapidapi.com/v1/fixtures/statistics"
    params = {"locale":"US","fixture_id":match_id}
    result = tm_fetch_data(url,params)
    result['match_id'] = match_id
    result['id'] = result.apply(lambda df: eval(f"{df['match_id']}{df['clubId']}"),axis=1)
    return result
    
@op
def tm_fetch_match(match_id):
    url = "https://transfermarkt-db.p.rapidapi.com/v1/fixtures/info"
    params = {"locale":"US","fixture_id": match_id}
    response = tm_api_call(url, params)
    data = response.json()['data']
    df = pd.DataFrame.from_dict(data, orient='index').T
    df['date'] = df['timestamp'].apply(lambda x: datetime.fromtimestamp(x))
    match_result_df = tm_fetch_match_result(match_id)
    result_df = pd.concat([df, match_result_df],axis=1)
    cols = ['id',
            'date',
            'postponed',
            'stadiumID', 
            'stadiumName', 
            'spectators', 
            'seasonID', 
            'competitionID', 
            'competitionName', 
            'competitionRound',
            'refereeID', 
            'homeTeamID', 
            'homeTeamName', 
            'awayTeamID', 
            'awayTeamName', 
            'firstLeg', 
            'nextRound',
            'goalsHome',
            'goalsAway', 
            'halftimeGoalsHome',
            'halftimeGoalsAway'
    ]
    result_df = result_df[cols]
    result_df['draw'] = result_df.apply(lambda x: True if x.goalsHome == x.goalsAway else False, axis=1)
    result_df['winning_team'] = result_df.apply(lambda x: match_result(x.goalsHome, x.homeTeamID, x.goalsAway, x.awayTeamID, 'win'), axis=1)
    result_df['losing_team'] = result_df.apply(lambda x: match_result(x.goalsHome, x.homeTeamID, x.goalsAway, x.awayTeamID, 'lose'), axis=1)
    return result_df

@op
def tm_fetch_player_profile(player_id):
    url = f"{tm_url}players/profile"
    params = {"locale":"US","player_id":player_id}
    response = tm_api_call(url, params)
    data = response.json()['data']['playerProfile']
    result = pd.DataFrame.from_dict(data, orient='index').T
    return result