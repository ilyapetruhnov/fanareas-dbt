import os
from dagster import Definitions, load_assets_from_modules, define_asset_job, AssetSelection, ScheduleDefinition
from dagster_dbt import DbtCliResource
from dagster_fanareas.assets import assets, dbt, core_assets
from dagster_fanareas.quizzes import templates, quiz_assets
from dagster_fanareas.facts import facts, fact_assets

from .constants import dbt_project_dir, POSTGRES_CONFIG
# from .schedules import schedules
from dagster_fanareas.resources.db_io_manager import db_io_manager

all_assets = load_assets_from_modules([assets, dbt, core_assets, templates, facts, fact_assets, quiz_assets])

# postgres_instance = db_io_manager.configured(POSTGRES_CONFIG)

# airbyte_assets = load_assets_from_airbyte_instance( airbyte_instance,  key_prefix=["src_postgres"])



guess_the_player_quiz_job = define_asset_job(name="quiz_guess_the_player", selection="post_guess_the_player_quiz")
transfers_quiz_job = define_asset_job(name="quiz_transfers", selection="post_transfers_quiz")


post_news_job = define_asset_job(name="post_news", selection="post_news")

templates_job = define_asset_job("templates_job", AssetSelection.groups("templates"))

news_schedule = ScheduleDefinition(job=post_news_job, cron_schedule="0 0,4,8,12,16,20 * * *")

quiz_schedule = ScheduleDefinition(job=transfers_quiz_job, cron_schedule="0 22 * * *")

defs = Definitions(
    assets=[*all_assets],
    jobs = [guess_the_player_quiz_job,
            transfers_quiz_job,
            post_news_job
            ],
    schedules=[news_schedule,
               quiz_schedule
               ],
    resources={
        "dbt": DbtCliResource(project_dir=os.fspath(dbt_project_dir)),
        "db_io_manager": db_io_manager.configured(POSTGRES_CONFIG)
    },
)

