import pandas as pd
import numpy as np

from dagster import (
    MetadataValue,
    Output,
    ResourceParam,
    asset,
)

from .. import dataframe_types, utility


@asset(description="Add missing columns, if any.")
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

    # Fill empty cells with default values
    cols_with_empty_values = df.columns[df.isnull().any()].tolist()
    for column in cols_with_empty_values:
        df[column] = df[column].fillna(default_column_values[column])

    df = df.astype(dtype=raw_column_dtypes)
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
def split_violation_categories(validate_columns_and_types: pd.DataFrame
                               ) -> Output[pd.DataFrame]:
    df = validate_columns_and_types[['violation', 'violation_status']]

    # Some values in violation and violation_status have sub_values, that are separated by a dash.
    # We're splitting them for easier reporting. Examples: 'NO PARKING-STREET CLEANING'
    df[['violation', 'sub_violation']] = (pd.Series(
        np.where(
            df['violation'].str.contains('-'),
            df['violation'],
            df['violation'] + '-Not specified'
        )
    )).str.split('-', n=1, expand=True)

    df[['violation_status', 'sub_violation_status']] = (pd.Series(  # TODO: Make DRY
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
        df,
        metadata={
            "num_records": len(df),
            "dtypes": MetadataValue.md((df.dtypes.to_markdown())),
            "preview": MetadataValue.md(df.head().to_markdown()),
        }
    )


@asset
def split_violation_time(validate_columns_and_types: pd.DataFrame,
                         ) -> Output[pd.DataFrame]:
    sr: pd.Series = validate_columns_and_types['violation_time']
    sr = sr.str.replace(' ', '')
    sr = sr.apply(lambda x: utility.get_reformatted_hour_minute(x))
    df: pd.DataFrame = pd.DataFrame(sr.tolist(), columns=['violation_hour', 'violation_minute'], index=sr.index)

    return Output(
        df,
        metadata={
            "num_records": len(df),
            "max_hour": df['violation_hour'].max(),
            "min_hour": df['violation_hour'].min(),
            "max_minute": df['violation_minute'].max(),
            "min_minute": df['violation_minute'].min(),
            "num_of_NaN": df.isna().sum().to_markdown(),
            "preview": MetadataValue.md(df.head().to_markdown()),
        }
    )


@asset(description="Fill empty cells with their corresponding default values.")
def merge_dfs(validate_columns_and_types: pd.DataFrame,
                      split_violation_time: pd.DataFrame,
                      split_violation_categories: pd.DataFrame,
                      default_column_values: ResourceParam[dict],
                      ) -> Output[dataframe_types.CleanedCameraViolationsDataframe]:
    df = validate_columns_and_types.drop(columns=['violation_time'])
    df[['violation',
        'sub_violation',
        'violation_status',
        'sub_violation_status']] = split_violation_categories[['violation',
                                                               'sub_violation',
                                                               'violation_status',
                                                               'sub_violation_status']]

    df[['violation_hour', 'violation_minute']] = split_violation_time[['violation_hour', 'violation_minute']]

    # cols_with_empty_values = df.columns[df.isnull().any()].tolist()
    # for column in cols_with_empty_values:
    #     df[column] = df[column].fillna(default_column_values[column])

    return Output(
        df,
        metadata={
            "num_records": len(df),
            "empty_values_per_column": MetadataValue.md((df.isna().sum().to_markdown())),
            "preview": MetadataValue.md(df.head().to_markdown()),
        }
    )
