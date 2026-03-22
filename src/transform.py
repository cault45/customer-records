import pandas as pd
import numpy as np


def transform(users_df: pd.DataFrame, transactions_df: pd.DataFrame) -> pd.DataFrame:

    users_df = users_df.copy()
    transactions_df = transactions_df.copy()

    df_merged = pd.merge(users_df, transactions_df, left_on='id', right_on='userId', how='left')

    row_count = df_merged[df_merged['id_y'].notnull()].groupby('id_x').size()
    df_merged['row_count'] = df_merged['id_x'].map(row_count).fillna(0)

    bins = [0, 7, 14, float('inf')]
    labels = ['LOW', 'MID', 'HIGH']

    df_merged['customer_segment'] = pd.cut(df_merged['row_count'], bins=bins, labels=labels, include_lowest=True, right=True)

    df_merged = df_merged.reset_index(drop=True)

    return df_merged


def summarise(df: pd.DataFrame) -> pd.DataFrame:

    df = df.copy()

    df = df.groupby(['city']).agg(
        total_customers_per_city=('id_x', 'nunique'),
        avg_trans=('row_count', lambda x: round(x.mean(), 2)),
        high_count=('customer_segment', lambda x: (x == 'HIGH').sum()),
        mid_count=('customer_segment', lambda x: (x == 'MID').sum()),
        low_count=('customer_segment', lambda x: (x == 'LOW').sum())
    )

    df = df.reset_index(drop=True)

    return df
