import os
from dagster import Definitions, load_assets_from_modules, define_asset_job, AssetSelection, ScheduleDefinition
from dagster_dbt import DbtCliResource
from dagster_fanareas.assets import assets, dbt, core_assets
from dagster_fanareas.quizzes import templates

from .constants import dbt_project_dir, POSTGRES_CONFIG
# from .schedules import schedules
from dagster_fanareas.resources.db_io_manager import db_io_manager

all_assets = load_assets_from_modules([assets, dbt, core_assets, templates])

# postgres_instance = db_io_manager.configured(POSTGRES_CONFIG)

# airbyte_assets = load_assets_from_airbyte_instance( airbyte_instance,  key_prefix=["src_postgres"])


quiz_player_shirt_number_job = define_asset_job(name="quiz_player_shirt_number_job", selection="post_quiz_player_shirt_number")
quiz_player_age_nationality_job = define_asset_job(name="quiz_player_age_nationality_job", selection="post_quiz_player_age_nationality")
quiz_player_age_team_job = define_asset_job(name="quiz_player_age_team_job", selection="post_quiz_player_age_team")
quiz_player_2_clubs_played_job = define_asset_job(name="quiz_player_2_clubs_played_job", selection="post_quiz_player_2_clubs_played")
quiz_player_transferred_from_to_job = define_asset_job(name="quiz_player_transferred_from_to_job", selection="post_quiz_player_transferred_from_to")

templates_job = define_asset_job("templates_job", AssetSelection.groups(templates))
templates_schedule = ScheduleDefinition(job=templates_job, cron_schedule="* * * * *")

defs = Definitions(
    assets=[*all_assets],
    jobs = [quiz_player_shirt_number_job,
            quiz_player_age_nationality_job,
            quiz_player_age_team_job,
            quiz_player_2_clubs_played_job,
            quiz_player_transferred_from_to_job,
            templates_job],
    schedules=[templates_schedule],
    resources={
        "dbt": DbtCliResource(project_dir=os.fspath(dbt_project_dir)),
        "db_io_manager": db_io_manager.configured(POSTGRES_CONFIG)
    },
)

