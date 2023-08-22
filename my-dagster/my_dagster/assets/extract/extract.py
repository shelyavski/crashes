import pandas as pd

from datetime import date, timedelta
from sodapy import Socrata
from dagster import (
    MetadataValue,
    Output,
    RetryPolicy,
    ResourceParam,
    asset,
)


@asset(description="Retrieve last week's camera & parking violations",
       retry_policy=RetryPolicy(
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

    return Output(
        df,
        metadata={
            "num_records": len(df),
            "dtypes": MetadataValue.md((df.dtypes.to_markdown())),
            "preview": MetadataValue.md(df.head().to_markdown()),
        }
    )
