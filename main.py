import sys
from datetime import datetime
from typing import List
from utils.auth import auth
from utils.bigquery import (
    download_plays,
    download_track_info,
    download_owners_info,
    download_payouts_info,
    write_df
)
from utils.logic import merge_tracks_info, create_report

N_ARGS = 3
DATE_FORMAT = "%Y-%m-%d"


def main(arguments: List[str]) -> None:
    print("Program execution started.")
    start_date, end_date, table = parse_and_validate_arguments(arguments)
    credentials, client, storage = auth()
    plays = download_plays(client, storage, start_date, end_date)
    track_info = download_track_info(client, storage)
    owners = download_owners_info(client, storage)
    payouts = download_payouts_info(client, storage, end_date)
    tracks_df = merge_tracks_info(plays, track_info, owners)
    report = create_report(tracks_df, payouts, start_date, end_date)
    write_df(credentials, report, table)


def parse_and_validate_arguments(
        arguments: List[str]
) -> (
        datetime.date,
        datetime.date,
        str
):
    assert len(arguments) == N_ARGS, f"The program requires {N_ARGS} arguments."
    reporting_start_str = arguments[0]
    reporting_end_str = arguments[1]
    try:
        reporting_start_date = datetime.strptime(reporting_start_str, DATE_FORMAT).date()
        reporting_end_date = datetime.strptime(reporting_end_str, DATE_FORMAT).date()
    except ValueError as e:
        print(e)
        print(f"The first two arguments must be dates. Required format: {DATE_FORMAT}")
        sys.exit()
    table_name = arguments[2]
    print("Program arguments successfully parsed.")
    return reporting_start_date, reporting_end_date, table_name


if __name__ == '__main__':
    main(sys.argv[1:])
