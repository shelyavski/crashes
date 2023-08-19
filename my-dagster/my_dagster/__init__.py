from dagster import (
    Definitions,
    AssetSelection,
    ScheduleDefinition,
    load_assets_from_modules,
    define_asset_job,
)

from . import assets, resources

all_assets = load_assets_from_modules([assets])

parking_violations_job = define_asset_job("parking_violations_job", selection=AssetSelection.all())
parking_violations_schedule = ScheduleDefinition(
    job=parking_violations_job,
    cron_schedule="0 15 * * *",
)

defs = Definitions(
    assets=all_assets,
    schedules=[parking_violations_schedule],
    resources={
        "socrata_client": resources.socrata_client,
        "raw_column_dtypes": resources.column_dtypes,
        "default_column_values": resources.default_column_values,
        "clickhouse_client": resources.clickhouse_client,
    },
)
