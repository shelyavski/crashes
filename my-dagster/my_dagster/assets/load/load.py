from clickhouse_connect.driver.httpclient import Client as ClickhouseClient
from dagster import (
    ResourceParam,
    asset,
)
from .. import dataframe_types


@asset(description="Create the DB and Table if they don't exist.")
def setup_dwh(clickhouse_client: ResourceParam[ClickhouseClient],
              create_nyc_data_database: ResourceParam[str],
              create_camera_violations_table: ResourceParam[str],
              ) -> None:
    clickhouse_client.command(create_nyc_data_database)
    clickhouse_client.command(create_camera_violations_table)


@asset(description="Load the final DataFrame in Clickhouse",
       deps=[setup_dwh])
def load_to_dwh(merge_dfs: dataframe_types.CleanedCameraViolationsDataframe,
                clickhouse_client: ResourceParam[ClickhouseClient]
                ) -> None:
    clickhouse_client.insert_df(
        df=merge_dfs,
        database="nyc_data",
        table="camera_violations"
    )
