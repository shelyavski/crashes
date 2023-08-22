import pandas as pd
import numpy as np

from dagster import (
    MetadataValue,
    Output,
    ResourceParam,
    asset,
)

from .. import dataframe_types


@asset(description="Add missing columns, if any.")
def validate_columns_and_types(
        get_records_as_df: pd.DataFrame,
        column_dtypes: ResourceParam[dict],
        default_column_values: ResourceParam[dict],
) -> Output[pd.DataFrame]:
    df = get_records_as_df

    # Check if all columns are
    actual_columns = df.columns.values.tolist()
    missing_columns = [col_name for col_name in column_dtypes.keys() if col_name not in actual_columns]

    # Add missing columns and fill with default value
    if missing_columns:
        for col_name in missing_columns:
            df[col_name] = df.apply(lambda _: default_column_values[col_name], axis=1)

    # Return with correct dtypes
    return Output(
        df,
        metadata={
            "num_records": len(df),
            "dtypes": MetadataValue.md((df.dtypes.to_markdown())),
            "preview": MetadataValue.md(df.head().to_markdown()),
        }
    )


@asset(description="Split violation & violation_status into sub_categories for easier analytics")
def split_violation_categories(validate_columns_and_types: pd.DataFrame,
                               column_dtypes: ResourceParam[dict]
                               ) -> Output[pd.DataFrame]:
    df = validate_columns_and_types
    df[['violation', 'violation_status']].fillna("Not specified")

    # Some values in violation and violation_status have sub_values, that are separated by a dash.
    # We're splitting them for easier reporting. Examples: 'NO PARKING-STREET CLEANING'
    df[['violation', 'sub_violation']] = (pd.Series(
        np.where(
            df['violation'].str.contains('-'),
            df['violation'],
            df['violation'] + '-Not specified'
        )
    )).str.split('-', n=1, expand=True)

    df[['violation_status', 'sub_violation_status']] = (pd.Series(
        np.where(
            df['violation_status'].str.contains('-'),
            df['violation_status'],
            df['violation_status'] + '-Not specified'
        )
    )).str.split('-', n=1, expand=True)

    df[['violation',
        'sub_violation',
        'violation_status',
        'sub_violation_status']] = df[['violation',
                                       'sub_violation',
                                       'violation_status',
                                       'sub_violation_status']].astype(dtype='string[pyarrow]')

    return Output(
        df.astype(dtype=column_dtypes),
        metadata={
            "num_records": len(df),
            "dtypes": MetadataValue.md((df.dtypes.to_markdown())),
            "preview": MetadataValue.md(df.head().to_markdown()),
        }
    )


@asset(description="Fill empty cells with their corresponding default values.")
def fill_empty_values(split_violation_categories: pd.DataFrame,
                      default_column_values: ResourceParam[dict],
                      ) -> Output[dataframe_types.CleanedCameraViolationsDataframe]:
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