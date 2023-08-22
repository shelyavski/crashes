from . import client_resources, sql_resources, validation_resources

resources = {
    "socrata_client": client_resources.socrata_client,
    "clickhouse_client": client_resources.clickhouse_client,
    "column_dtypes": validation_resources.column_dtypes,
    "default_column_values": validation_resources.default_column_values,
    "create_nyc_data_database": sql_resources.create_nyc_data_database,
    "create_camera_violations_table": sql_resources.create_camera_violations_table,
}