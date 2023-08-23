from dagster import load_assets_from_package_module
from . import extract, transform, load

EXTRACT = "Extract"
LOAD = "Load"
TRANSFORM = "Transform"

extract_assets = load_assets_from_package_module(package_module=extract, group_name=EXTRACT)
transform_assets = load_assets_from_package_module(package_module=transform, group_name=TRANSFORM)
load_assets = load_assets_from_package_module(package_module=load, group_name=LOAD)
