from dagster import asset
import pandas as pd
from dagster_fanareas.ops.tm_utils import tm_fetch_data, rename_camel_col, tm_fetch_squads, tm_fetch_player_performance, tm_fetch_match, tm_fetch_match_stats, tm_fetch_player_profile
from dagster_fanareas.constants import tm_url

@asset(group_name="ingest_v2", compute_kind="pandas", io_manager_key="new_io_manager")
def season(context) -> pd.DataFrame:
    url = f"{tm_url}competitions/seasons"

    params = {"locale":"US","competition_id":"GB1"}

    df = tm_fetch_data(url ,params)
    return df

@asset(group_name="ingest_v2", compute_kind="pandas", io_manager_key="new_io_manager")
def standing(context) -> pd.DataFrame:
    existing_df = context.resources.new_io_manager.load_table(table_name='season')
    seasons = existing_df['id'].unique()
    frames = []
    for i in seasons:
        params = {"locale":"US",
                    "season_id": i,
                    "standing_type":"general",
                    "competition_id":"GB1"}

        url = f"{tm_url}competitions/standings"
        df = tm_fetch_data(url,params,key='table')
        df['season_id'] = i
        df['season_id'] = df['season_id'].astype(int)
        df.rename(columns={"id": 'team_id'},inplace=True)
        df.rename(columns={"group": 'group_id'},inplace=True)
        for col in df.columns:
            new_col_name = rename_camel_col(col)
            df.rename(columns={col: new_col_name},inplace=True)
        df['team_id'] = df['team_id'].astype(int)
        df['id'] = df['team_id'] + df['season_id']
        frames.append(df)
    result = pd.concat(frames)
    return result

@asset(group_name="ingest_v2", compute_kind="pandas", io_manager_key="new_io_manager")
def squad(context) -> pd.DataFrame:
    # existing_df = context.resources.new_io_manager.load_table(table_name='season')
    season_id = 2023
    teams = ['11','31']
    frames = []
    for i in teams:
        df = tm_fetch_squads(season_id=season_id, team_id=i)
        for col in df.columns:
            new_col_name = rename_camel_col(col)
            df.rename(columns={col: new_col_name},inplace=True)
            df.rename(columns={"id": 'player_id'},inplace=True)
        frames.append(df)
    result = pd.concat(frames)
    return result

@asset(group_name="ingest_v2", compute_kind="pandas", io_manager_key="new_io_manager")
def player_performace(context) -> pd.DataFrame:
    # existing_df = context.resources.new_io_manager.load_table(table_name='season')
    season_id = '2023'
    players = ['331560','451276']
    frames = []
    for i in players:
        df = tm_fetch_player_performance(season_id=season_id, player_id=i)
        for col in df.columns:
            new_col_name = rename_camel_col(col)
            df.rename(columns={col: new_col_name},inplace=True)
        frames.append(df)
    result = pd.concat(frames)
    return result

@asset(group_name="ingest_v2", compute_kind="pandas", io_manager_key="new_io_manager")
def matches(context) -> pd.DataFrame:
    existing_df = context.resources.new_io_manager.load_table(table_name='player_performace')
    matches = existing_df['id'].unique()
    frames = []
    for match_id in matches:
        df = tm_fetch_match(match_id)
        for col in df.columns:
            new_col_name = rename_camel_col(col)
            df.rename(columns={col: new_col_name},inplace=True)
        frames.append(df)
    result = pd.concat(frames)
    return result

@asset(group_name="ingest_v2", compute_kind="pandas", io_manager_key="new_io_manager")
def match_stats(context) -> pd.DataFrame:
    existing_df = context.resources.new_io_manager.load_table(table_name='matches')
    matches = existing_df['id'].unique()
    frames = []
    for match_id in matches:
        df = tm_fetch_match_stats(match_id)
        df.rename(columns={'clubId': 'team_id'},inplace=True)
        frames.append(df)
    result = pd.concat(frames)
    cols = ['id','team_id', 'ballpossession', 'offsides', 'fouls', 'freekicks',
       'cornerkicks', 'goalkeepersaves', 'shotsoffgoal', 'shotsongoal',
       'shotstotal']
    result = result[cols]
    return result

@asset(group_name="ingest_v2", compute_kind="pandas", io_manager_key="new_io_manager")
def player(context) -> pd.DataFrame:
    existing_df = context.resources.new_io_manager.load_table(table_name='squad')
    players = existing_df['player_id'].unique()
    frames = []
    for i in players:
        df = tm_fetch_player_profile(i)
        df.rename(columns={'playerID': 'id'},inplace=True)
        df.rename(columns={'clubId': 'team_id'},inplace=True)
        df.rename(columns={'club': 'team'},inplace=True)
        frames.append(df)
    result = pd.concat(frames)
    cols = [
        'id',
        'player_image',
        'player_name',
        'player_full_name',
        'birthplace',
        'date_of_birth',
        'date_of_death',
        'player_shirt_number',
        'birthplace_country',
        'age',
        'height',
        'foot',
        'international_team',
        'country',
        'second_country',
        'league',
        'team',
        'team_id',
        'contract_expiry_date',
        'agent',
        'agent_id',
        'position_group',
        'player_main_position',
        'player_second_position',
        'player_third_position',
        'market_value',
        'market_value_currency',
        'market_value_numeral',
        'market_value_last_change']
    result = result[cols]
    return result