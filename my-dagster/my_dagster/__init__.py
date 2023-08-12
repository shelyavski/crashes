from dagster import (
    Definitions,
    AssetSelection,
    ScheduleDefinition,
    load_assets_from_modules,
    define_asset_job,
)

from . import assets

all_assets = load_assets_from_modules([assets])

parking_violations_job = define_asset_job("parking_violations_job", selection=AssetSelection.all())
parking_violations_schedule = ScheduleDefinition(
    job=parking_violations_job,
    cron_schedule='0 15 * * *',
)

defs = Definitions(
    assets=all_assets,
    schedules=[parking_violations_schedule],
)
