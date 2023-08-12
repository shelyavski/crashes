import os
import pandas as pd

from sodapy import Socrata
from dotenv import load_dotenv
from datetime import date,timedelta

from dagster import (
    MetadataValue,
    Output,
    RetryPolicy,
    asset,
)

# TODO: Add validations: https://docs.dagster.io/integrations/pandas


@asset(retry_policy=RetryPolicy(
    max_retries=3,
    delay=5))
def get_records_as_df() -> Output[pd.DataFrame]:
    # Get environment variables from the .env file
    load_dotenv(dotenv_path='../.env')

    # Instantiate the Socrata client
    client = Socrata(
        domain="data.cityofnewyork.us",
        app_token=os.environ['APP_TOKEN'],
        username=os.environ['U_NAME'],
        password=os.environ['PASS']
    )

    yesterday = (date.today() - timedelta(days=1)).strftime('%m/%d/%Y')

    # Query the API
    try:
        records = client.get(
            dataset_identifier="nc67-uf89",
            where=f"issue_date = '{yesterday}'"
        )
    except ConnectionError:
        raise ConnectionError

    # Transform to data frame
    df = pd.DataFrame.from_records(records)

    return Output(df, metadata={
            "num_records": len(df),
            "preview": MetadataValue.md(df.head().to_markdown()),
        })


@asset
def split_violation_categories(get_records_as_df: pd.DataFrame) -> Output[pd.DataFrame]:
    df = get_records_as_df

    # Some values in violation and violation_status have sub-values, that are separated by a dash.
    # We're splitting them for easier reporting. Examples: 'NO PARKING-STREET CLEANING'
    df[['violation', 'sub-violation']] = df['violation'].str.split('-', expand=True)
    df[['violation_status', 'sub-violation_status']] = df['violation_status'].str.split('-', expand=True)

    return Output(df, metadata={
            "num_records": len(df),
            "df_columns": MetadataValue.md((df.dtypes.to_markdown())),
            "preview": MetadataValue.md(df.head().to_markdown()),
        })


@asset
def fill_empty_values(split_violation_categories: pd.DataFrame) -> Output[pd.DataFrame]:
    columns_with_empty_values = (
        "plate",
        "state",
        "license_type",
        "summons_number",
        "violation",
        "precinct",
        "county",
        "issuing_agency",
        "violation_status",
        "summons_image",
        "sub-violation",
        "sub-violation_status"
    )

    df = split_violation_categories

    for column in columns_with_empty_values:
        df[column] = df[column].fillna('Not specified')

    # Using 01/01/1970 instead of NaN in Clickhouse Date fields
    df['judgment_entry_date'] = df['judgment_entry_date'].fillna('01/01/1970')

    return Output(df, metadata={
            "num_records": len(df),
            "empty_values_per_column": MetadataValue.md((df.isna().sum().to_markdown())),
            "preview": MetadataValue.md(df.head().to_markdown()),
        }
    )


# @asset
# def persist_in_clickhouse(fill_empty_values: pd.DataFrame) -> None:
#     x = fill_empty_values
#     pass