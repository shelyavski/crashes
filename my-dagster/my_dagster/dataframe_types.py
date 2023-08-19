import datetime

from dagster_pandas import create_dagster_pandas_dataframe_type, PandasColumn

string_constraints = {
    'non_nullable': True,
    'is_required': True
}

float_constraints = {
    'min_value': 0,
    'non_nullable': True,
    'is_required': True
}

date_constraints = {
    'min_datetime': datetime.datetime(
        year=1970,
        month=1,
        day=1
    ), 'non_nullable': True,
    'is_required': True
}

cleaned_camera_violations_columns: list[PandasColumn] = [
    PandasColumn.string_column(
        'plate',
        **string_constraints
    ),
    PandasColumn.string_column(
        'state',
        **string_constraints
    ),
    PandasColumn.string_column(
        'license_type',
        **string_constraints
    ),
    PandasColumn.string_column(
        'summons_number',
        **string_constraints
    ),
    PandasColumn.datetime_column(
        'issue_date',
        **date_constraints
    ),
    PandasColumn.string_column(
        'violation',
        **string_constraints
    ),
    PandasColumn.string_column(
        'violation_time',
        **string_constraints
    ),
    PandasColumn.float_column(
        'fine_amount',
        **float_constraints
    ),
    PandasColumn.float_column(
        'penalty_amount',
        **float_constraints
    ),
    PandasColumn.float_column(
        'interest_amount',
        **float_constraints
    ),
    PandasColumn.float_column(
        'reduction_amount',
        **float_constraints
    ),
    PandasColumn.float_column(
        'payment_amount',
        **float_constraints
    ),
    PandasColumn.float_column(
        'amount_due',
        **float_constraints
    ),
    PandasColumn.string_column(
        'precinct',
        **string_constraints
    ),
    PandasColumn.string_column(
        'county',
        **string_constraints
    ),
    PandasColumn.string_column(
        'issuing_agency',
        **string_constraints
    ),
    PandasColumn.string_column(
        'violation_status',
        **string_constraints
    ),
    PandasColumn.string_column(
        'summons_image',
        **string_constraints
    ),
    PandasColumn.datetime_column(
        'judgment_entry_date',
        **date_constraints
    ),
    PandasColumn.string_column(
        'sub-violation_status',
        **string_constraints
    ),
    PandasColumn.string_column(
        'sub-violation',
        **string_constraints
    ),
]

CleanedCameraViolationsDataframe = create_dagster_pandas_dataframe_type(
    name='CleanedCameraViolationsDataframe',
    columns=cleaned_camera_violations_columns
)
