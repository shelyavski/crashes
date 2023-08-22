from dagster import (
    AssetSelection,
    ScheduleDefinition,
    define_asset_job,
)
from .assets import EXTRACT, LOAD, TRANSFORM


parking_violations_job = define_asset_job(
    name="parking_violations_job",
    selection=AssetSelection.groups(EXTRACT) | AssetSelection.groups(LOAD) | AssetSelection.groups(TRANSFORM)
)

parking_violations_schedule = ScheduleDefinition(
    job=parking_violations_job,
    cron_schedule="0 15 * * *",
)