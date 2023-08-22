import os
import clickhouse_connect

from sodapy import Socrata
from dotenv import load_dotenv
from dagster import resource

load_dotenv(dotenv_path='.env')

# ------------ CLIENT RESOURCES ------------
socrata_client = Socrata(
    domain="data.cityofnewyork.us",
    app_token=os.environ.get("SOCRATA_APP_TOKEN"),
    username=os.environ.get("SOCRATA_U_NAME"),
    password=os.environ.get("SOCRATA_PASS")
)

clickhouse_client = clickhouse_connect.get_client(
    host="localhost",
    port=8123,
    username=os.environ.get("CLICKHOUSE_U_NAME"),
    password=os.environ.get("CLICKHOUSE_PASS")
)