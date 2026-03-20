import pandas as pd


def transform(users_df: pd.DataFrame, transactions_df: pd.DataFrame) -> pd.DataFrame:


    users_df = users_df.copy()
    transactions_df = transactions_df.copy()

    df_merged = pd.merge(users_df, transactions_df, left_on='id', right_on='userId', how='left')

    row_count = df_merged.groupby('id_x').agg(count().astype(int)
    df_merged['row_count'] = df_merged['id_x'].map(row_count)

    # df_merged['trans_count'] = df_merged.groupby('id_x').size()

    print(df_merged)

