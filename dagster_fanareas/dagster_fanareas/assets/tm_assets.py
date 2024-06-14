from dagster import asset
import pandas as pd
from dagster_fanareas.ops.utils import tm_fetch_data
from dagster_fanareas.constants import tm_url

@asset(group_name="ingest_v2", compute_kind="pandas", io_manager_key="new_io_manager")
def seasons(context) -> pd.DataFrame:
    url = f"{tm_url}competitions/seasons"

    params = {"locale":"US","competition_id":"GB1"}

    df = tm_fetch_data(url ,params)
    return df

@asset(group_name="ingest_v2", compute_kind="pandas", io_manager_key="new_io_manager")
def rankings(context) -> pd.DataFrame:
    existing_df = context.resources.db_io_manager.load_table(table_name='seasons')
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
        df['team_id'] = df['team_id'].astype(int)
        df['id'] = df['team_id'] + df['season_id']
        frames.append(df)
    result = pd.concat(frames)
    return result