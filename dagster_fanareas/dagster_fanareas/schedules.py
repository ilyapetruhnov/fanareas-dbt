"""
To add a daily schedule that materializes your dbt assets, uncomment the following lines.
"""

from dagster import ScheduleDefinition

from dagster_dbt import build_schedule_from_dbt_selection

from dagster_fanareas.assets.dbt import fanareas_dbt_assets
from .definitions import templates_job



templates_schedule = ScheduleDefinition(job=templates_job, cron_schedule="* * * * *")



schedules = [
    build_schedule_from_dbt_selection(
        [fanareas_dbt_assets],
        job_name="materialize_dbt_models",
        cron_schedule="0 0 * * *",
        dbt_select="fqn:*",
    ),
]

