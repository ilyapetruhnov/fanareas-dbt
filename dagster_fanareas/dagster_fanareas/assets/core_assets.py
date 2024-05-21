from dagster import asset
import pandas as pd
from dagster_fanareas.ops.utils import fetch_data, upsert
from dagster_fanareas.constants import core_url


@asset(compute_kind="pandas", io_manager_key="db_io_manager")
def leagues(context) -> pd.DataFrame:
    df = context.resources.db_io_manager.upsert_input(context)
    return df

@asset( group_name="ingest", compute_kind="pandas", io_manager_key="db_io_manager")
def stages(context) -> pd.DataFrame:
    df = context.resources.db_io_manager.upsert_input(context)
    return df

@asset( group_name="ingest", compute_kind="pandas", io_manager_key="db_io_manager")
def rounds(context) -> pd.DataFrame:
    df = context.resources.db_io_manager.upsert_input(context)
    return df

@asset( group_name="ingest", compute_kind="pandas", io_manager_key="db_io_manager")
def venues(context) -> pd.DataFrame:
    df = context.resources.db_io_manager.upsert_input(context)
    return df

@asset( group_name="ingest", compute_kind="pandas", io_manager_key="db_io_manager")
def referees(context) -> pd.DataFrame:
    df = context.resources.db_io_manager.upsert_input(context)
    return df

@asset(compute_kind="pandas", io_manager_key="db_io_manager")
def countries() -> pd.DataFrame:
    df = fetch_data(f"{core_url}/countries?filters=populate")
    return df

@asset(compute_kind="pandas", io_manager_key="db_io_manager")
def regions() -> pd.DataFrame:
    df = fetch_data(f"{core_url}/regions?filters=populate")
    return df

@asset(compute_kind="pandas", io_manager_key="db_io_manager")
def cities() -> pd.DataFrame:
    df = fetch_data(f"{core_url}/cities?filters=populate")
    return df

@asset(compute_kind="pandas", io_manager_key="db_io_manager")
def continents() -> pd.DataFrame:
    df = fetch_data(f"{core_url}/continents?filters=populate")
    return df

@asset(compute_kind="pandas", io_manager_key="db_io_manager")
def types() -> pd.DataFrame:
    df = fetch_data(f"{core_url}/types?filters=populate")
    return df