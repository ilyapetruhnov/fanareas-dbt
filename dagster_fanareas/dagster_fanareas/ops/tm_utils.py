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
    response = tm_api_call(url, params)
    try:
        result = response.json()['data']
        if key is not None:
            data.append(result.get(key))
        else:
            data.append(result)
        result_df = pd.DataFrame(list(chain(*data)))
    except Exception as e:
        result_df = pd.DataFrame([])
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
    try:
        data = response.json()['data']
        result_df = pd.DataFrame.from_dict(data, orient='index').T
    except Exception as e:
        result_df = pd.DataFrame([])
    return result_df
    

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
    try:
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
    except Exception as e:
        result_df = pd.DataFrame([])
    return result_df

@op
def tm_fetch_player_profile(player_id):
    url = f"{tm_url}players/profile"
    params = {"locale":"US","player_id":player_id}
    response = tm_api_call(url, params)
    data = response.json()['data']['playerProfile']
    result = pd.DataFrame.from_dict(data, orient='index').T
    return result

@op
def tm_fetch_team_profile(team_id):
    url = f"{tm_url}clubs/profile"
    params = {"locale":"US","club_id":team_id}
    response= tm_api_call(url, params)
    
    facts = response.json()['data']['mainFacts']
    facts_df = pd.DataFrame.from_dict(facts, orient='index').T
    cols = ['city','founding','avgAge']
    facts_df = facts_df[cols]

    stadium = response.json()['data']['stadium']
    stadium_df = pd.DataFrame.from_dict(stadium, orient='index').T
    stadium_df.rename(columns={'id': 'stadium_id','image': 'stadium_image','name': 'stadium_name'},inplace=True)

    stadium_cols = ['stadium_id', 'stadium_name',
       'constructionYear', 'totalCapacity', 'standingRoom', 'seats',
       'stadium_image']
    stadium_df = stadium_df[stadium_cols]
    result = pd.concat([facts_df,stadium_df],axis=1)
    return result

@op
def tm_fetch_team_info(team_id):
    url = f"{tm_url}clubs/info"
    params = {"locale":"US","club_id":team_id}
    response= tm_api_call(url, params)
    data = response.json()['data']
    result_df = pd.DataFrame.from_dict(data, orient='index').T
    cols = ['id', 
            'name', 
            'image', 
            'countryID', 
            'leagueID', 
            'leagueName', 
            'coachID', 
            'coachName',  
            'marketValue', 
            'marketValueCurrency', 
            'marketValueNumeral']
    result_df = result_df[cols]
    result_df.rename(columns={'countryID': 'country_id','leagueID': 'league_id','coachID': 'coach_id'}, inplace=True)
    return result_df

@op
def tm_fetch_team_transfers(team_id):
    url = f"{tm_url}transfers/list"
    page_num = 0
    frames = []
    while True:
        params = {
            "locale":"US",
            "club_id": team_id,
            "page_number": page_num,
            "top_transfers_first": "false"
                  }
        page_num +=1
        response= tm_api_call(url, params)
        data = response.json()['data']
        if len(data) == 0:
            break

        frames.append(pd.DataFrame(data))
    if len(frames)>0:
        df = pd.concat(frames)
    else:
        return None
    df['transferFee_value'] = df['transferFee'].apply(lambda x: x['value'])
    df['transferFee_currency'] = df['transferFee'].apply(lambda x: x['currency'])
    df['transferMarketValue_value'] = df['transferMarketValue'].apply(lambda x: x['value'])
    df['transferMarketValue_currency'] = df['transferMarketValue'].apply(lambda x: x['currency'])
    cols = ['id', 'playerID', 'fromClubID', 'toClubID', 'transferredAt', 'isLoan',
        'wasLoan', 'season', 'fromCompetitionID', 'toCompetitionID','transferFee_value',
        'transferFee_currency', 'transferMarketValue_value',
        'transferMarketValue_currency']
    df = df[cols].reset_index()
    df.rename(columns={'fromClubID': 'from_team_id',
                    'toClubID': 'to_club_id',
                    'playerID': 'player_id',
                    'fromCompetitionID':'from_competition_id',
                    'toCompetitionID':'to_competition_id'
                    }, inplace=True)
    return df

@op
def tm_fetch_titles(team_id):
    url = f"{tm_url}clubs/profile"
    params = {"locale":"US","club_id":team_id}
    result = tm_api_call(url, params)
    if result is None:
        return None
    frames = []
    data = result.json()['data']['successes']
    cols = ['number', 'name', 'id', 'competition_id',
    'competition_type_id', 'cycle', 'seasonIds']

    for i in data:
        result_df = pd.DataFrame.from_dict(i, orient='index').T
        result_df['competition_id'] = result_df['additionalData'].apply(lambda x:x['competitionId'])
        result_df['competition_type_id'] = result_df['additionalData'].apply(lambda x:x['competitionTypeId'])
        result_df['cycle'] = result_df['additionalData'].apply(lambda x:x['cycle'])
        result_df['seasonIds'] = result_df['additionalData'].apply(lambda x:x['seasonIds'])
        result_df = result_df.explode('seasonIds',ignore_index=True)
        frames.append(result_df[cols])
    if len(frames)>0:
        df = pd.concat(frames)
        df['team_id'] = team_id
        return df
    else:
        return None

@op
def tm_fetch_countries():
    url = f"{tm_url}static/countries"
    params = {"locale":"US"}
    result = tm_api_call(url, params)
    return result.json()['de']

@op
def tm_fetch_competitions(country_id):
    url = f"{tm_url}countries/competitions"
    params = {"country_id":country_id,"locale":'US'}
    result = tm_api_call(url, params)
    return result.json()['data']
