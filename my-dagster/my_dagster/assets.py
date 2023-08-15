import pandas as pd
import numpy as np

from datetime import date, timedelta
from sodapy import Socrata
from dagster import (
    MetadataValue,
    Output,
    RetryPolicy,
    ResourceParam,
    asset,
)


# TODO: Add custom validation types: https://docs.dagster.io/integrations/pandas


@asset(retry_policy=RetryPolicy(
    max_retries=1,  # TODO: Change retries and delay once finished testing
    delay=15)
)
def get_records_as_df(socrata_client: ResourceParam[Socrata]) -> Output[pd.DataFrame]:
    yesterday = (date.today() - timedelta(days=1)).strftime("%m/%d/%Y")

    # Query the API
    try:
        records = socrata_client.get(
            dataset_identifier="nc67-uf89",
            where=f"issue_date = '{yesterday}'",
            limit=5  # TODO: Change limit once finished testing
        )
    except ConnectionError:
        raise ConnectionError

    # Transform to data frame
    df = pd.DataFrame.from_records(data=records)

    df['violation_time'] = df['violation_time'] + 'M'  # So that we get AM and PM instead of A and P

    return Output(
        df,
        metadata={
            "num_records": len(df),
            "dtypes": MetadataValue.md((df.dtypes.to_markdown())),
            "preview": MetadataValue.md(df.head().to_markdown()),
        }
    )


@asset
def validate_columns_and_types(
        get_records_as_df: pd.DataFrame,
        raw_column_dtypes: ResourceParam[dict],
        default_column_values: ResourceParam[dict],
) -> Output[pd.DataFrame]:
    df = get_records_as_df

    # Check if all columns are
    actual_columns = df.columns.values.tolist()
    missing_columns = [col_name for col_name in raw_column_dtypes.keys() if col_name not in actual_columns]

    # Add missing columns and fill with default value
    if missing_columns:
        for col_name in missing_columns:
            df[col_name] = df.apply(lambda _: default_column_values[col_name], axis=1)

    # Return with correct dtypes
    return Output(
        df.astype(dtype=raw_column_dtypes),
        metadata={
            "num_records": len(df),
            "dtypes": MetadataValue.md((df.dtypes.to_markdown())),
            "preview": MetadataValue.md(df.head().to_markdown()),
        }
    )


@asset
def split_violation_categories(validate_columns_and_types: pd.DataFrame) -> Output[pd.DataFrame]:
    df = validate_columns_and_types

    # Some values in violation and violation_status have sub-values, that are separated by a dash.
    # We're splitting them for easier reporting. Examples: 'NO PARKING-STREET CLEANING'
    df[['violation', 'sub-violation']] = (pd.Series(
        np.where(
            df['violation'].str.contains('-'),
            df['violation'],
            df['violation'] + '-Not specified'
        )
    )).str.split('-', n=1, expand=True)

    df[['violation_status', 'sub-violation_status']] = (pd.Series(
        np.where(
            df['violation_status'].str.contains('-'),
            df['violation_status'],
            df['violation_status'] + '-Not specified'
        )
    )).str.split('-', n=1, expand=True)

    return Output(
        df,
        metadata={
            "num_records": len(df),
            "dtypes": MetadataValue.md((df.dtypes.to_markdown())),
            "preview": MetadataValue.md(df.head().to_markdown()),
        }
    )


@asset
def fill_empty_values(split_violation_categories: pd.DataFrame,
                      default_column_values: ResourceParam[dict],
                      ) -> Output[pd.DataFrame]:

    df = split_violation_categories

    for column in df.columns:
        df[column] = df[column].fillna(default_column_values[column])

    return Output(
        df,
        metadata={
            "num_records": len(df),
            "empty_values_per_column": MetadataValue.md((df.isna().sum().to_markdown())),
            "preview": MetadataValue.md(df.head().to_markdown()),
        }
    )
