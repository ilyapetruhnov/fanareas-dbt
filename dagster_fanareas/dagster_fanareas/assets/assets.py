from dagster import asset
import pandas as pd
from dagster_fanareas.ops.utils import api_call, fetch_data, flatten_list, upsert
from dagster_fanareas.constants import base_url, api_key
from itertools import product
import time

@asset( group_name="transfers", compute_kind="pandas", io_manager_key="db_io_manager")
def transfers(context) -> pd.DataFrame:
    df = context.resources.db_io_manager.upsert_input(context)
    return df

@asset( group_name="seasons", compute_kind="pandas", io_manager_key="db_io_manager")
def seasons(context) -> pd.DataFrame:
    df = context.resources.db_io_manager.upsert_input(context)
    return df


@asset( group_name="standings", compute_kind="pandas", io_manager_key="db_io_manager")
def standings(context) -> pd.DataFrame:
    df = context.resources.db_io_manager.upsert_input(context)
    return df

@asset( group_name="fixtures", compute_kind="pandas")
def fixtures_df() -> pd.DataFrame:
    fixtures_url = f"{base_url}/fixtures?filters=fixtureLeagues:8&filters=populate"
    df = fetch_data(fixtures_url)
    return df

@asset( group_name="fixtures", compute_kind="pandas", io_manager_key="db_io_manager")
def fixtures(context, fixtures_df: pd.DataFrame) -> pd.DataFrame:
    existing_df = context.resources.db_io_manager.load_input(context)
    return upsert(existing_df, fixtures_df)


@asset( group_name="coaches", compute_kind="pandas", io_manager_key="db_io_manager")
def coaches(context) -> pd.DataFrame:
    df = context.resources.db_io_manager.upsert_input(context)
    return df

@asset( group_name="teams", compute_kind="pandas", io_manager_key="db_io_manager")
def teams(context) -> pd.DataFrame:
    dataset_name = context.asset_key.path[-1]
    try:
        existing_df = context.resources.db_io_manager.load_input(context)
        context.log.info(existing_df.head())
        if existing_df.empty == True:
            url = f"{base_url}/{dataset_name}"
        else:
            last_id = max(existing_df['id'])
            url = f"{base_url}/{dataset_name}?filters=idAfter:{last_id}"
    except Exception as e:
        existing_df = pd.DataFrame([])
        url = f"{base_url}/{dataset_name}"
    context.log.info(url)
    context.log.info(api_key)
    context.log.info('pulling data')  
    df = fetch_data(context, url, api_key)
    context.log.info(df.head())
    return df


@asset(group_name="squads", compute_kind="pandas")
def squads_df(seasons, teams) -> pd.DataFrame:
    season_ids = list(seasons['id'].unique())
    team_ids = list(teams['id'].unique())
    combinations = list(product(season_ids, team_ids))
    squads = []
    for season_id, team_id in combinations:
        players_url = f"{base_url}/squads/seasons/{season_id}/teams/{team_id}"
        response_players = api_call(players_url)
        try:
            squads.append(response_players.json()['data'])
        except Exception as e:
            pass
    squads_records = set()
    data_size = len(squads)
    for i in range(data_size):
        data_sz = len(squads[i])
        for item in range(data_sz):
            squads_records.add(tuple(squads[i][item].values()))
    squads_fields = tuple(squads[0][0].keys())
    df = pd.DataFrame.from_records(list(squads_records), columns=squads_fields)
    return df


@asset( group_name="squads", compute_kind="pandas", io_manager_key="db_io_manager")
def squads(context, squads_df: pd.DataFrame) -> pd.DataFrame:
    existing_df = context.resources.db_io_manager.load_input(context)
    return upsert(existing_df, squads_df)

@asset(group_name="topscorers", compute_kind="pandas")
def topscorers_list(context, seasons) -> list:
    season_ids = list(seasons['id'].unique())
    topscorers = []
    for season_id in season_ids:
        topscorers_url = f"{base_url}/topscorers/seasons/{season_id}"
        response = api_call(topscorers_url)
        try:
            topscorers.append(response.json()['data'])
        except Exception as e:
            pass
        limit = response.json()['rate_limit']['remaining']
        if limit == 1:
            seconds_until_reset = response.json()['rate_limit']['resets_in_seconds']
            context.log.info(seconds_until_reset)
            time.sleep(seconds_until_reset)
            continue
        else:
            continue
    return flatten_list(topscorers)

@asset(group_name="topscorers", compute_kind="pandas")
def topscorers_df(topscorers_list: list) -> pd.DataFrame:
    return pd.DataFrame(topscorers_list)

@asset( group_name="topscorers", compute_kind="pandas", io_manager_key="db_io_manager")
def topscorers(context, topscorers_df: pd.DataFrame) -> pd.DataFrame:
    existing_df = context.resources.db_io_manager.load_input(context)
    context.log.info(topscorers_df.head())
    return upsert(existing_df, topscorers_df)

@asset( group_name="players", compute_kind="pandas", io_manager_key="db_io_manager")
def players(context) -> pd.DataFrame:
    df = context.resources.db_io_manager.upsert_input(context)
    return df

@asset( group_name="player_stats", compute_kind="pandas")
def player_stats_dict(context, players: pd.DataFrame) -> dict:
    player_ids = list(players['id'].unique())
    player_stats = []
    player_stats_detailed = []
    context.log.info(len(player_ids))
    for player_id in player_ids:
        url = f"{base_url}/statistics/seasons/players/{player_id}"
        response = api_call(url)
        try:
            player_stats.append([i for i in response.json()['data']])
            player_stats_detailed.append([i['details'] for i in response.json()['data']])
        except Exception as e:
            pass
        limit = response.json()['rate_limit']['remaining']
        context.log.info(limit)
        if limit == 1:
            seconds_until_reset = response.json()['rate_limit']['resets_in_seconds']
            context.log.info(seconds_until_reset)
            time.sleep(seconds_until_reset+1)
        else:
            continue
    return {'stats': player_stats, 'detailed_stats': player_stats_detailed}

@asset( group_name="player_stats", compute_kind="pandas", io_manager_key="db_io_manager")
def player_stats(context, player_stats_dict: dict) -> pd.DataFrame:
    player_stats = player_stats_dict['stats']
    result = flatten_list(player_stats)
    df = pd.DataFrame(result)
    df = df.drop('details', axis=1)
    existing_df = context.resources.db_io_manager.load_input(context)
    return upsert(existing_df, df)

@asset( group_name="player_stats", compute_kind="pandas",io_manager_key="db_io_manager")
def player_stats_detailed(context, player_stats_dict: dict) -> pd.DataFrame:
    player_stats_detailed = player_stats_dict['detailed_stats']
    result = flatten_list(player_stats_detailed)
    df = pd.DataFrame(result)
    df['total'] = df['value'].apply(lambda x: x['total'] if 'total' in x.keys() else None)
    df['goals'] = df['value'].apply(lambda x: x['goals'] if 'goals' in x.keys() else None)
    df['penalties'] = df['value'].apply(lambda x: x['penalties'] if 'penalties' in x.keys() else None)
    df['home'] = df['value'].apply(lambda x: x['home'] if 'home' in x.keys() else None)
    df['away'] = df['value'].apply(lambda x: x['away'] if 'away' in x.keys() else None)
    df = df.drop('value', axis=1)
    existing_df = context.resources.db_io_manager.load_input(context)
    return upsert(existing_df, df)

@asset( group_name="team_stats", compute_kind="pandas")
def team_stats_dict(context, teams: pd.DataFrame) -> dict:
    team_ids = list(teams['id'].unique())
    context.log.info(team_ids)
    team_stats = []
    team_stats_detailed = []
    for team_id in team_ids:
        url = f"{base_url}/statistics/seasons/teams/{team_id}"
        response = api_call(url)
        try:
            team_stats.append([i for i in response.json()['data']])
            team_stats_detailed.append([i['details'] for i in response.json()['data']])
        except Exception as e:
            pass
        
        limit = response.json()['rate_limit']['remaining']
        context.log.info(limit)
        if limit == 1:
            return {'stats': team_stats, 'detailed_stats': team_stats_detailed}
    return {'stats': team_stats, 'detailed_stats': team_stats_detailed}


@asset( group_name="team_stats", compute_kind="pandas",io_manager_key="db_io_manager")
def team_stats(context, team_stats_dict: dict) -> pd.DataFrame:
    team_stats = team_stats_dict['stats']
    result = flatten_list(team_stats)
    df = pd.DataFrame(result)
    df = df.drop('details', axis=1)
    existing_df = context.resources.db_io_manager.load_input(context)

    outer = existing_df.merge(df, how='outer', indicator=True)
    diff = outer[(outer._merge=='left_only')].drop('_merge', axis=1)
    return diff


@asset( group_name="team_stats", compute_kind="pandas",io_manager_key="db_io_manager")
def team_stats_detailed(context, team_stats_dict: dict) -> pd.DataFrame:
    team_stats_detailed = flatten_list(team_stats_dict['detailed_stats'])
    df = pd.json_normalize(team_stats_detailed)
    context.log.info(df.head())
    cols = [i.replace('.','_').replace('-','_') for i in df.columns]
    df.columns = cols
    existing_df = context.resources.db_io_manager.load_input(context)
    outer = existing_df.merge(df, how='outer', indicator=True)
    diff = outer[(outer._merge=='left_only')].drop('_merge', axis=1)
    return diff

