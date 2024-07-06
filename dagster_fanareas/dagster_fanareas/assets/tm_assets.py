from dagster import asset
import pandas as pd
from dagster_fanareas.ops.tm_utils import tm_fetch_data, rename_camel_col, tm_fetch_squads, tm_fetch_player_performance, tm_fetch_match, tm_fetch_match_stats, tm_fetch_player_profile, tm_fetch_team_profile, tm_fetch_team_info, tm_fetch_team_transfers, tm_fetch_titles, tm_fetch_countries, tm_fetch_competitions, tm_fetch_stuff
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
        df['id'] = df.apply(lambda df: eval(f"{df['season_id']}{df['team_id']}"),axis=1)
        frames.append(df)
    return pd.concat(frames)

@asset(group_name="ingest_v2", compute_kind="pandas", io_manager_key="new_io_manager")
def squad(context) -> pd.DataFrame:
    standing_df = context.resources.new_io_manager.load_table(table_name='standing')
    team_seasons = list(zip(standing_df['team_id'], standing_df['season_id']))
    frames = []
    for i in team_seasons:
        team_id = i[0]
        season_id = i[1]
        df = tm_fetch_squads(season_id=season_id, team_id=team_id)
        for col in df.columns:
            new_col_name = rename_camel_col(col)
            df.rename(columns={col: new_col_name},inplace=True)
            df['id'] = df.apply(lambda df: eval(f"{df['player_id']}{df['team_id']}{df['season_id']}"),axis=1)
        frames.append(df)
    result = pd.concat(frames)
    return result

@asset(group_name="ingest_v2", compute_kind="pandas", io_manager_key="new_io_manager")
def player_performace(context) -> pd.DataFrame:
    squad_df = context.resources.new_io_manager.load_table(table_name='squad')
    player_seasons = list(zip(squad_df['player_id'], squad_df['season_id']))
    frames = []
    for i in player_seasons:
        player_id = i[0]
        season_id = i[1]
        try:
            df = tm_fetch_player_performance(season_id=season_id, player_id=player_id)
            for col in df.columns:
                new_col_name = rename_camel_col(col)
                df.rename(columns={col: new_col_name},inplace=True)
            frames.append(df)
        except Exception as e:
            pass
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
    cols = ['id','match_id','team_id', 'ballpossession', 'offsides', 'fouls', 'freekicks',
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
        df.rename(columns={'playerID': 'id','clubID': 'team_id','club': 'team'},inplace=True)
        for col in df.columns:
            new_col_name = rename_camel_col(col)
            df.rename(columns={col: new_col_name},inplace=True)
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

@asset(group_name="ingest_v2", compute_kind="pandas", io_manager_key="new_io_manager")
def team(context) -> pd.DataFrame:
    existing_df = context.resources.new_io_manager.load_table(table_name='standing')
    teams = existing_df['team_id'].unique()
    frames = []
    for i in teams:
        df_profile = tm_fetch_team_profile(i)
        df_info = tm_fetch_team_info(i)
        df =  pd.concat([df_profile, df_info],axis=1)
        for col in df.columns:
            new_col_name = rename_camel_col(col)
            df.rename(columns={col: new_col_name},inplace=True)
        frames.append(df)
    return pd.concat(frames)

@asset(group_name="ingest_v2", compute_kind="pandas", io_manager_key="new_io_manager")
def transfer(context) -> pd.DataFrame:
    existing_df = context.resources.new_io_manager.load_table(table_name='team')
    teams = existing_df['id'].unique()
    frames = []
    for i in teams:
        df = tm_fetch_team_transfers(i)
        if df is not None:
            for col in df.columns:
                new_col_name = rename_camel_col(col)
                df.rename(columns={col: new_col_name},inplace=True)
            frames.append(df)
    return pd.concat(frames)

@asset(group_name="ingest_v2", compute_kind="pandas", io_manager_key="new_io_manager")
def titles(context) -> pd.DataFrame:
    existing_df = context.resources.new_io_manager.load_table(table_name='team')
    teams = existing_df['id'].unique()
    frames = []
    for i in teams:
        context.log.info(i)
        df = tm_fetch_titles(i)
        if df is not None:
            for col in df.columns:
                new_col_name = rename_camel_col(col)
                df.rename(columns={col: new_col_name},inplace=True)
            frames.append(df)
    return pd.concat(frames)

@asset(group_name="ingest_v2", compute_kind="pandas", io_manager_key="new_io_manager")
def stuff(context) -> pd.DataFrame:
    frames = []
    existing_df = context.resources.new_io_manager.load_table(table_name='team')
    coaches = existing_df['coach_id'].unique()
    for stuff_id in coaches:
        context.log.info(stuff_id)
        data = tm_fetch_stuff(stuff_id)
        if data is not None:
            result_df = pd.DataFrame.from_dict(data, orient='index').T
            cols = ['id', 'countryID', 'personID', 'personImage','playerID', 'personnelID',
        'personName', 'firstName', 'lastName', 'alias', 'dateOfBirth',
        'deathDay', 'age', 'birthplace',  'countryImage',
        'countryName', 'averageTermAsCoach']
            result_df = result_df[cols]
            result_df.rename(columns={'personID': 'id',
                                    'countryID': 'country_id',
                                    'playerID': 'player_id',
                                    'personnelID': 'personnel_id'},inplace=True)

            frames.append(result_df)
    return pd.concat(frames)


@asset(group_name="ingest_v2", compute_kind="pandas", io_manager_key="new_io_manager")
def country() -> pd.DataFrame:
    frames = []
    data = tm_fetch_countries()
    for i in data:
        result_df = pd.DataFrame.from_dict(i, orient='index').T
        frames.append(result_df)
    return pd.concat(frames)

@asset(group_name="ingest_v2", compute_kind="pandas", io_manager_key="new_io_manager")
def competitions(context) -> pd.DataFrame:
    existing_df = context.resources.new_io_manager.load_table(table_name='country')
    countries = existing_df['id'].unique()
    frames = []
    result = []
    for country_id in countries:
        try:
            data = tm_fetch_competitions(country_id)
            for i in data['children']:
                result_df = pd.DataFrame.from_dict(i, orient='index').T
                frames.append(result_df)
            df = pd.concat(frames)
            df['country'] = data['competitionGroupName']
            df.rename(columns={'competitionGroupCompetitionID': 'competitionGroupCompetition_id'},inplace=True)
            for col in df.columns:
                new_col_name = rename_camel_col(col)
                df.rename(columns={col: new_col_name},inplace=True)
                result.append(df)
        except TypeError:
            pass
    return pd.concat(result)