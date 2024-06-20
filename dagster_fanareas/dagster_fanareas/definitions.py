import os
from dagster import Definitions, load_assets_from_modules, define_asset_job, AssetSelection, ScheduleDefinition
from dagster_dbt import DbtCliResource, build_dbt_asset_selection

from dagster_fanareas.assets.dbt import fanareas_dbt_assets

from dagster_fanareas.assets import assets, dbt, core_assets, tm_assets
from dagster_fanareas.quizzes import quiz_assets
from dagster_fanareas.facts import facts, fact_assets

from .constants import dbt_project_dir, POSTGRES_CONFIG, NEW_POSTGRES_CONFIG
# from .schedules import schedules
from dagster_fanareas.resources.db_io_manager import db_io_manager

all_assets = load_assets_from_modules([tm_assets, assets, dbt, core_assets, facts, fact_assets, quiz_assets])

dbt_selection = build_dbt_asset_selection(
    [fanareas_dbt_assets]
    # dbt_select="tag:daily"
)

# postgres_instance = db_io_manager.configured(POSTGRES_CONFIG)

# airbyte_assets = load_assets_from_airbyte_instance( airbyte_instance,  key_prefix=["src_postgres"])



guess_the_player_quiz_job = define_asset_job(name="trigger_guess_team_player_quiz", selection="guess_team_player_quiz")

transfers_quiz_job = define_asset_job(name="trigger_transfers_quiz", selection="transfers_quiz")

photo_quiz_job = define_asset_job(name="trigger_photo_quiz", selection="photo_quiz")

guess_the_team_quiz_job = define_asset_job(name="trigger_team_quiz", selection="guess_the_team_quiz")

post_news_job = define_asset_job(name="trigger_post_news", selection="post_news")

# post_facts_job = define_asset_job(name="trigger_post_facts", selection="publish_one_fact")
post_team_facts_job = define_asset_job(name="trigger_post_team_facts", selection="publish_team_fact")

post_facts_by_team_job = define_asset_job(name="trigger_post_facts_by_team", selection="publish_one_fact_by_team")

post_facts_player_season_job = define_asset_job(name = "trigger_post_facts_by_player_season", selection="publish_player_season_stats_fact")

ingest_job = define_asset_job("ingest_job", AssetSelection.groups("ingest"))

dbt_job = define_asset_job("daily_dbt_assets", selection=dbt_selection)

news_schedule = ScheduleDefinition(
    job=post_news_job, 
    cron_schedule="0 0,2,4,6,8,10,12,14,16,18,20,22 * * *"
)

transfers_quiz_schedule = ScheduleDefinition(
    job=transfers_quiz_job, 
    cron_schedule="0 6 */4 * *"
)

photo_quiz_schedule = ScheduleDefinition(
    job=photo_quiz_job, 
    cron_schedule="0 6 * * *"
)

guess_the_team_quiz_schedule = ScheduleDefinition(
    job=guess_the_team_quiz_job, 
    cron_schedule="0 6 */3 * *"
)

guess_the_player_quiz_schedule = ScheduleDefinition(
    job=guess_the_player_quiz_job, 
    cron_schedule="0 6 */3 * *"
)

facts_by_team_schedule = ScheduleDefinition(
    job=post_facts_by_team_job, 
    cron_schedule="0 6 * * *"
)

team_facts_schedule = ScheduleDefinition(
    job=post_team_facts_job, 
    cron_schedule="0 6 */2 * *"
)

facts_player_season_schedule = ScheduleDefinition(
    job=post_facts_player_season_job, 
    cron_schedule="10 6 * * *"
)

daily_dbt_assets_schedule = ScheduleDefinition(
    job=dbt_job,
    cron_schedule="15 7 * * *"
)

daily_ingest_assets_schedule = ScheduleDefinition(
    job=ingest_job,
    cron_schedule="0 7 * * *"
)


defs = Definitions(
    assets=[*all_assets],
    jobs = [guess_the_player_quiz_job,
            guess_the_team_quiz_job,
            transfers_quiz_job,
            photo_quiz_job,
            post_news_job,
            post_team_facts_job,
            post_facts_by_team_job,
            post_facts_player_season_job,
            dbt_job,
            ingest_job
            ],
    schedules=[news_schedule,
               transfers_quiz_schedule,
               photo_quiz_schedule,
               guess_the_player_quiz_schedule,
               guess_the_team_quiz_schedule,
               facts_by_team_schedule,
               team_facts_schedule,
               facts_player_season_schedule,
               daily_dbt_assets_schedule,
               daily_ingest_assets_schedule
               ],
    resources={
        "dbt": DbtCliResource(project_dir=os.fspath(dbt_project_dir)),
        "db_io_manager": db_io_manager.configured(POSTGRES_CONFIG),
        "new_io_manager": db_io_manager.configured(NEW_POSTGRES_CONFIG)
    },
)

