# import pandas as pd
# from sqlalchemy import create_engine
# from dagster import get_dagster_logger
# from dagster import AssetKey, EnvVar

# # from dagster_embedded_elt.sling import (SlingResource, SlingSourceConnection, SlingTargetConnection)
from dagster_fanareas.ops.utils import fetch_data, upsert
from dagster_fanareas.constants import base_url
import pandas as pd
from dagster import IOManager, io_manager
from dagster import (
    IOManager,
    io_manager,
)


class DbIOManager(IOManager):
    """Sample IOManager to handle loading the contents of tables as pandas DataFrames.

    Does not handle cases where data is written to different schemas for different outputs, and
    uses the name of the asset key as the table name.
    """

    def __init__(self, con_string: str):
        self._con = con_string

    def handle_output(self, context, obj):
        if isinstance(obj, pd.DataFrame):
            # write df to table
            obj.set_index('id').to_sql(name=context.asset_key.path[-1], con=self._con, if_exists="append")
        elif obj is None or obj.empty == True:
            # dbt has already written the data to this table
            pass
        else:
            raise ValueError(f"Unsupported object type {type(obj)} for DbIOManager.")

    def load_input(self, context) -> pd.DataFrame:
        """Load the contents of a table as a pandas DataFrame."""
        model_name = context.asset_key.path[-1]
        #context.add_output_metadata({"table_name": model_name})
        return pd.read_sql(f"SELECT * FROM {model_name}", con=self._con)
    
    def load_players_unnest_query(self, input_team_id):

        """Load the contents of a table as a pandas DataFrame."""
        #context.add_output_metadata({"table_name": model_name})
        query = f"""
                    with vw as (SELECT player_id
                                    , firstname
                                    , lastname
                                    , fullname
                                    , nationality
                                    , date_of_birth
                                    , array_to_string(t.team, ',')          as team
                                    , array_to_string(t.team_id, ',')       as team_id
                                    , array_to_string(t.jersey_number, ',') as jersey_number
                                    , t.season
                                    , t.captain
                                    , t.yellow_cards
                                    , t.red_cards
                                    , t.yellow_red_cards
                                    , t.minutes_played
                                    , t.appearances
                                    , t.assists
                                    , t.lineups
                                    , t.goals
                                    , t.home_yellow_cards
                                    , t.penalties
                                    , t.own_goals
                                    , t.goals_conceded
                                FROM dim_players
                                        CROSS JOIN UNNEST(season_stats) AS t
                                WHERE current_season = 2023
                                and t.season = 2023)
                    select * from vw
                    WHERE
                    team_id = {input_team_id}
        """
        return pd.read_sql(query, con=self._con)

    
    def load_players_two_clubs_query(self):

        """Load the contents of a table as a pandas DataFrame."""
        #context.add_output_metadata({"table_name": model_name})
        query = f"""
                    with vw as (SELECT player_id
                        , lastname
                        , fullname
                        , nationality
                        , date_of_birth
                        , t.team                                as team_arr
                        , array_to_string(t.team, ',')          as team
                        , array_to_string(t.team_id, ',')       as team_id
                        , array_to_string(t.jersey_number, ',') as jersey_number
                        , t.season
                        , t.captain
                        , t.yellow_cards
                        , t.red_cards
                        , t.yellow_red_cards
                        , t.minutes_played
                        , t.appearances
                        , t.assists
                        , t.lineups
                        , t.goals
                        , t.home_yellow_cards
                        , t.penalties
                        , t.own_goals
                        , t.goals_conceded
                    FROM dim_players
                            CROSS JOIN UNNEST(season_stats) AS t
                    WHERE current_season = 2023
                    )
        select *
        from vw
        where array_length(team_arr, 1) > 1
        """
        return pd.read_sql(query, con=self._con)

    def load_table_by_id(self, context, input_id) -> pd.DataFrame:
        """Load the contents of a table as a pandas DataFrame."""
        model_name = context.asset_key.path[-1]
        #context.add_output_metadata({"table_name": model_name})
        return pd.read_sql(f"SELECT * FROM {model_name} WHERE id = {input_id}", con=self._con)
    
    def upsert_input(self, context) -> pd.DataFrame:
        dataset_name = context.asset_key.path[-1]
        try:
            existing_df = self.load_input(context)
            last_id = max(existing_df['id'])
            url = f"{base_url}/{dataset_name}?filters=idAfter:{last_id}"
        except Exception as e:
            existing_df = pd.DataFrame([])
            url = f"{base_url}/{dataset_name}"
        context.log.info(url)   
        new_df = fetch_data(context, url)
        context.log.info(new_df.empty)
        merged_df = upsert(existing_df, new_df)
        context.log.info(merged_df.head())
        return merged_df


@io_manager(config_schema={"con_string": str})
def db_io_manager(context):
    return DbIOManager(context.resource_config["con_string"])

