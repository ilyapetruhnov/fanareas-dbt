import os
from dagster import Definitions, load_assets_from_modules
from dagster_dbt import DbtCliResource
from dagster_fanareas.assets import assets, dbt, core_assets
from .constants import dbt_project_dir, POSTGRES_CONFIG
from .schedules import schedules
from dagster_fanareas.resources.db_io_manager import db_io_manager

all_assets = load_assets_from_modules([assets, dbt, core_assets])

defs = Definitions(
    assets=[*all_assets],
    schedules=schedules,
    resources={
        "dbt": DbtCliResource(project_dir=os.fspath(dbt_project_dir)),
        "db_io_manager": db_io_manager.configured(POSTGRES_CONFIG)
    },
)

