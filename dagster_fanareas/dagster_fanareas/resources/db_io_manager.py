# import pandas as pd
# from sqlalchemy import create_engine
# from dagster import get_dagster_logger
# from dagster import AssetKey, EnvVar

# # from dagster_embedded_elt.sling import (SlingResource, SlingSourceConnection, SlingTargetConnection)
from dagster_fanareas.ops.utils import fetch_data, upsert
from dagster_fanareas.constants import base_url, api_key
import pandas as pd
import sqlalchemy
from sqlalchemy import text
import uuid
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
        if isinstance(obj, pd.DataFrame) and obj.empty:
            pass
        # dbt has already written the data to this table
        elif isinstance(obj, pd.DataFrame) and obj.empty == False:
            # write df to table
            obj.set_index('id').to_sql(name=context.asset_key.path[-1], con=self._con, if_exists="append")
        else:
            raise ValueError(f"Unsupported object type {type(obj)} for DbIOManager.")
        

    def load_input(self, context) -> pd.DataFrame:
        """Load the contents of a table as a pandas DataFrame."""
        model_name = context.asset_key.path[-1]
        #context.add_output_metadata({"table_name": model_name})
        return pd.read_sql(f"SELECT * FROM {model_name}", con=self._con)

    def load_table_by_id(self, context, input_id) -> pd.DataFrame:
        """Load the contents of a table as a pandas DataFrame."""
        model_name = context.asset_key.path[-1]
        #context.add_output_metadata({"table_name": model_name})
        return pd.read_sql(f"SELECT * FROM {model_name} WHERE id = {input_id}", con=self._con)
    
    def upsert_input(self, context) -> pd.DataFrame:
        dataset_name = context.asset_key.path[-1]
        try:
            existing_df = self.load_input(context)
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
        df = fetch_data(url, api_key)
        context.log.info(df.head())
        # merged_df = upsert(existing_df, new_df)
        # context.log.info(merged_df.head())
        return df


@io_manager(config_schema={"con_string": str})
def db_io_manager(context):
    return DbIOManager(context.resource_config["con_string"])

