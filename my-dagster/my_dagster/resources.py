import os

from sodapy import Socrata
from dotenv import load_dotenv


load_dotenv(dotenv_path='.env')

socrata_client = Socrata(
    domain="data.cityofnewyork.us",
    app_token=os.environ.get("SOCRATA_APP_TOKEN"),
    username=os.environ.get("SOCRATA_U_NAME"),
    password=os.environ.get("SOCRATA_PASS")
)

