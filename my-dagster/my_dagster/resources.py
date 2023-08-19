import os
import numpy as np
import clickhouse_connect

from sodapy import Socrata
from dotenv import load_dotenv

load_dotenv(dotenv_path='.env')

# CLIENT RESOURCES
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

# DATAFRAME VALIDATION RESOURCES
column_dtypes = {
    'plate': 'string[pyarrow]',
    'state': 'string[pyarrow]',
    'license_type': 'string[pyarrow]',
    'summons_number': 'string[pyarrow]',
    'issue_date': 'datetime64[ns]',
    'violation': 'string[pyarrow]',
    'violation_time': 'string[pyarrow]',
    'fine_amount': 'float32[pyarrow]',
    'penalty_amount': 'float32[pyarrow]',
    'interest_amount': 'float32[pyarrow]',
    'reduction_amount': 'float32[pyarrow]',
    'payment_amount': 'float32[pyarrow]',
    'amount_due': 'float32[pyarrow]',
    'precinct': 'string[pyarrow]',
    'county': 'string[pyarrow]',
    'issuing_agency': 'string[pyarrow]',
    'violation_status': 'string[pyarrow]',
    'summons_image': 'string[pyarrow]',
    'judgment_entry_date': 'datetime64[ns]'
}

default_type_values = {
    'category': 'Not specified',
    'date': '01/01/1970',
    'time': '00:00AM',
    'float': np.nan
}

default_column_values = {
    'plate': default_type_values['category'],
    'state': default_type_values['category'],
    'license_type': default_type_values['category'],
    'summons_number': default_type_values['category'],
    'issue_date': default_type_values['date'],
    'violation': default_type_values['category'],
    'violation_time': default_type_values['time'],
    'fine_amount': default_type_values['float'],
    'penalty_amount': default_type_values['float'],
    'interest_amount': default_type_values['float'],
    'reduction_amount': default_type_values['float'],
    'payment_amount': default_type_values['float'],
    'amount_due': default_type_values['float'],
    'precinct': default_type_values['category'],
    'county': default_type_values['category'],
    'issuing_agency': default_type_values['category'],
    'violation_status': default_type_values['category'],
    'summons_image': default_type_values['category'],
    'judgment_entry_date': default_type_values['date'],
    'sub_violation': default_type_values['category'],
    'sub_violation_status': default_type_values['category'],
}
