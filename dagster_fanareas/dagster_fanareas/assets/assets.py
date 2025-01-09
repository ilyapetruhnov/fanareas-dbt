from dagster import asset
import pandas as pd
from dagster_fanareas.ops.utils import api_call, fetch_data, flatten_list, upsert, call_url
from dagster_fanareas.constants import base_url
import time

@asset( group_name="ingest", compute_kind="pandas", io_manager_key="db_io_manager")
def transfers(context) -> pd.DataFrame:
    df = context.resources.db_io_manager.upsert_input(context)
    return df

@asset( group_name="ingest", compute_kind="pandas", io_manager_key="db_io_manager")
def seasons(context) -> pd.DataFrame:
    df = context.resources.db_io_manager.upsert_input(context)
    return df


@asset( group_name="ingest", compute_kind="pandas", io_manager_key="db_io_manager")
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

@asset( group_name="ingest", compute_kind="pandas", io_manager_key="db_io_manager")
def coaches(context) -> pd.DataFrame:
    df = context.resources.db_io_manager.upsert_input(context)
    return df

@asset( group_name="ingest", compute_kind="pandas", io_manager_key="db_io_manager")
def teams(context) -> pd.DataFrame:
    df = context.resources.db_io_manager.upsert_input(context)
    return df

@asset( group_name="squads", compute_kind="pandas", io_manager_key="db_io_manager")
def squads(context, teams: pd.DataFrame) -> pd.DataFrame:
    season_id = 21646
    teams = teams[teams['short_code'].notna()]
    team_ids = list(teams['id'].unique())
    squads = []
    for team_id in team_ids:
        players_url = f"{base_url}/squads/seasons/{season_id}/teams/{team_id}"
        response_players = api_call(players_url)
        try:
            squads.append(response_players.json()['data'])
        except Exception as e:
            pass
    flattened = flatten_list(squads)
    squads_df = pd.DataFrame(flattened)
    existing_df = context.resources.db_io_manager.load_input(context)
    df = upsert(squads_df, existing_df)
    return df

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

@asset( group_name="ingest", compute_kind="pandas", io_manager_key="db_io_manager")
def players(context) -> pd.DataFrame:
    df = context.resources.db_io_manager.upsert_input(context)
    return df

@asset( group_name="ingest", compute_kind="pandas")
def player_stats_dict(context, players: pd.DataFrame) -> dict:
    dim_players = context.resources.db_io_manager.load_table(table_name = 'dim_players')
    non_active_player_ids = set(dim_players[(dim_players['current_season']==2023) & (dim_players['is_active']==False)]['player_id'].unique())
    all_player_ids = set(players['id'].unique())
    active_player_ids = all_player_ids.difference(non_active_player_ids)
    season_id = 21646
    player_stats = []
    player_stats_detailed = []
    context.log.info(len(active_player_ids))
    for player_id in active_player_ids:
        url = f"{base_url}/statistics/seasons/players/{player_id}?filters=playerstatisticSeasons:{season_id}"
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


@asset( group_name="ingest", compute_kind="pandas", io_manager_key="db_io_manager")
def player_stats(context, player_stats_dict: dict) -> pd.DataFrame:
    player_stats = player_stats_dict['stats']
    result = flatten_list(player_stats)
    df = pd.DataFrame(result)
    df = df.drop('details', axis=1)
    existing_df = context.resources.db_io_manager.load_input(context)
    context.log.info(df.head())
    df = upsert(df, existing_df)
    return df

@asset( group_name="ingest", compute_kind="pandas",io_manager_key="db_io_manager")
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
    context.log.info(df.head())
    df = upsert(df, existing_df)
    return df

@asset( group_name="ingest", compute_kind="pandas")
def team_stats_dict(context, teams: pd.DataFrame) -> dict:
    teams = teams[teams['short_code'].notnull()]
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


@asset( group_name="ingest", compute_kind="pandas",io_manager_key="db_io_manager")
def team_stats(context, team_stats_dict: dict) -> pd.DataFrame:
    team_stats = team_stats_dict['stats']
    context.log.info(team_stats)
    result = flatten_list(team_stats)
    context.log.info(result)
    df = pd.DataFrame(result)
    context.log.info(df.head())
    df = df.drop('details', axis=1)
    existing_df = context.resources.db_io_manager.load_input(context)
    context.log.info(df.head())
    df = upsert(df, existing_df)
    return df


@asset( group_name="ingest", compute_kind="pandas",io_manager_key="db_io_manager")
def team_stats_detailed(context, team_stats_dict: dict) -> pd.DataFrame:
    team_stats_detailed = flatten_list(team_stats_dict['detailed_stats'])
    df = pd.json_normalize(team_stats_detailed)
    context.log.info(df.head())
    cols = [i.replace('.','_').replace('-','_') for i in df.columns]
    df.columns = cols
    # dataset_name = context.asset_key.path[-1]
    existing_df = context.resources.db_io_manager.load_input(context)
    context.log.info(df.head())
    df = upsert(df, existing_df)
    return df


@asset( group_name="news")
def post_news() -> bool:
    url = "https://fanareas.com/api/news/parseNews"
    call_url(url)
    return True

@asset(group_name="tokens")
def post_tokens() -> bool:
    url = "https://fanareas.com/api/users/sendTokens"
    call_url(url)
    return True



