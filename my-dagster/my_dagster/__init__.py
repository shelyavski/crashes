from dagster import Definitions

from .resources import resources
from .jobs import parking_violations_schedule
from .assets import extract_assets, transform_assets, load_assets

etl_assets = [*extract_assets, *transform_assets, *load_assets]

defs = Definitions(
    assets=etl_assets,
    schedules=[parking_violations_schedule],
    resources=resources
)

