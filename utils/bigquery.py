# Download query results.
from datetime import date, timedelta
import pandas as pd
from google.cloud import bigquery
from google.cloud import bigquery_storage

BIGQUERY_DATE_FORMAT = "%Y%m%d"
# Min song play duration to be counted
MIN_DURATION = 30


def download_plays(
        client: bigquery.Client,
        storage: bigquery_storage.BigQueryReadClient,
        start_date: date,
        end_date: date
) -> pd.DataFrame:
    dataframes = []
    dates_diff = end_date - start_date
    for date_diff in range(dates_diff.days + 1):
        date_i = start_date + timedelta(days=date_diff)
        date_converted = date_i.strftime(BIGQUERY_DATE_FORMAT)
        query_string = f"""
        SELECT *
        FROM `rnr-data-eng-challenge.challenge_dataset.plays_{date_converted}`
        WHERE duration >= {MIN_DURATION};
        """
        dataframe = (
            client.query(query_string)
                .result()
                .to_dataframe(bqstorage_client=storage)
        )
        dataframes.append(dataframe)
    concatenated_df = pd.concat(dataframes)
    print("Tracks plays data successfully received.")
    return concatenated_df


def download_track_info(
        client: bigquery.Client,
        storage: bigquery_storage.BigQueryReadClient
) -> pd.DataFrame:
    query_string = """
    SELECT *
    FROM `rnr-data-eng-challenge.challenge_dataset.track_information_rollup`;
    """
    dataframe = (
        client.query(query_string)
            .result()
            .to_dataframe(bqstorage_client=storage)
    )
    print("Tracks titles successfully received.")
    return dataframe


def download_owners_info(
        client: bigquery.Client,
        storage: bigquery_storage.BigQueryReadClient
) -> pd.DataFrame:
    query_string = """
    SELECT *
    FROM `rnr-data-eng-challenge.challenge_dataset.track_owners_rollup`;
    """
    dataframe = (
        client.query(query_string)
            .result()
            .to_dataframe(bqstorage_client=storage)
    )
    print("Tracks owners successfully received.")
    return dataframe


def download_payouts_info(
        client: bigquery.Client,
        storage: bigquery_storage.BigQueryReadClient,
        end_date: date
) -> pd.DataFrame:

    end_date = end_date.strftime(BIGQUERY_DATE_FORMAT)
    query_string = f"""
        SELECT *
        FROM `rnr-data-eng-challenge.challenge_dataset.weekly_payout_{end_date}`;
        """
    dataframe = (
        client.query(query_string)
            .result()
            .to_dataframe(bqstorage_client=storage)
    )
    print("Payouts data successfully received.")
    return dataframe


def write_df(credentials, df: pd.DataFrame, table_name: str):
    df.to_gbq(
        destination_table=f'inr014.{table_name}',
        project_id='rnr-data-eng-challenge',
        if_exists="replace",
        table_schema=[
            {
                'name': 'reporting_start_date',
                'type': bigquery.enums.SqlTypeNames.DATE
            },
            {
                'name': 'reporting_end_date',
                'type': bigquery.enums.SqlTypeNames.DATE
            }
        ],
        credentials=credentials
    )
    print("Successfully wrote data to BigQuery.")
