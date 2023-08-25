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
           max_retries=3,
           delay=5)
       )
def get_records_as_df(socrata_client: ResourceParam[Socrata]) -> Output[pd.DataFrame]:
    today = date.today().strftime("%m/%d/%Y")
    a_week_ago = (date.today() - timedelta(days=8)).strftime("%m/%d/%Y")
    filter_query = f"issue_date >= '{a_week_ago}' AND issue_date < '{today}'"

    # Query the API
    try:
        records = socrata_client.get(
            dataset_identifier="nc67-uf89",
            where=filter_query,
            limit=100_000_000
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
