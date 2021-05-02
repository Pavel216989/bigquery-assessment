import pandas as pd
from datetime import date


def merge_tracks_info(
        plays: pd.DataFrame,
        track_info: pd.DataFrame,
        owners: pd.DataFrame
) -> pd.DataFrame:
    dataset = pd.merge(
        left=plays,
        right=track_info,
        how='left',
        on='track_id',
        sort=False,

    )
    dataset = dataset[
        (dataset['event_time'] >= dataset['valid_from'])
        & (dataset['event_time'] < dataset['valid_to'])
    ]
    dataset_final = pd.merge(
        left=dataset,
        right=owners,
        how='left',
        on='track_id',
        suffixes=['_track', '_owner'],
        sort=False
    )
    dataset_final = dataset_final[~dataset_final['owner_id'].isna()]
    dataset_final = dataset_final[
        (dataset_final['event_time'] >= dataset_final['valid_from_owner'])
        & (dataset_final['event_time'] < dataset_final['valid_to_owner'])
    ]
    dataset_final['owner_id'] = dataset_final['owner_id'].astype('int')
    return dataset_final


def create_report(
        tracks_df: pd.DataFrame,
        payouts: pd.DataFrame,
        start_date: date,
        end_date: date
) -> pd.DataFrame:
    payments_report = tracks_df.groupby(
        ["owner_id"])['track_id'].size().reset_index(name='total_plays')
    payments_report = pd.merge(
        left=payments_report,
        right=payouts,
        how='inner',
        on='owner_id',
        sort=False
    )
    price_per_play = payments_report['amount'] / payments_report['total_plays']
    payments_report['price_per_play'] = price_per_play.astype("float").round(decimals=2)
    tracks_report = tracks_df.groupby(
        ['track_id', 'owner_id', 'track_title']).size().reset_index(name='total_plays')
    report_merged = pd.merge(
        left=tracks_report,
        right=payments_report[['owner_id', 'price_per_play']],
        on='owner_id',
        sort=False
    )
    report_merged['reporting_start_date'] = start_date
    report_merged['reporting_end_date'] = end_date
    report_merged = report_merged[
        [
            'reporting_start_date', 'reporting_end_date',
            'track_id', 'track_title',
            'owner_id', 'price_per_play', 'total_plays'
        ]
    ]
    print("Financial report successfully created.")
    return report_merged
