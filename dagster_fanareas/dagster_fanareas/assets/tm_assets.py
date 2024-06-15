from dagster import asset
import pandas as pd
from dagster_fanareas.ops.utils import tm_fetch_data, rename_camel_col, tm_fetch_squads
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
        frames.append(df)
    result = pd.concat(frames)
    return result