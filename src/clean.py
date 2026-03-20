

def clean_data(users: , transactions):

    df_users = pd.DataFrame(users)
    df_transactions = pd.DataFrame(transactions)

    df_users['rejection_reason'] = np.where(
        (df['email'].str.contains('@')) &
        (df['email'].str.contains('\.')),
        None,
        "Invalid email format"
    )

    df_users['rejection_reason'] = np.where(
        (df_users['id'].isnull()) |
        (df_users['name'].isnull()) |
        (df_users['email'].isnull()),
        df_users['rejection_reason']
        "NULL value found in key column"
    )

    df_transactions['rejection_reason'] = np.where(
        (df_transactions['userId'].isnull()) |
        (df_transactions['id'].isnull()),
        None,
        "NULL value found in key columm"
    )

    df_users_rejected = df_users[df_users['rejection_reason'].notnull()]
    df_users = df_users[df_users['rejection_reason'].isnull()]

    df_transactions_rejected = df_transactions[df_transactions['rejection_reason'].notnull()]
    df_transactions = df_transactions[df_transactions['rejection_reason'].isnull()]

    df_users['name'] = df_users['name'].str.title()
    df_users['email'] = df_users['email'].str.lower()

    df_users = df_users.drop('rejection_reason', axis='columns')
    df_transactions = df_transactions.drop('rejection_reason', axis='columns')

    df_users = df_users.reset_index(drop=True)
    df_users_rejected = df_users_rejected.reset_index(drop=True)

    df_transactions = df_transactions.reset_index(drop=True)
    df_transactions_rejected = df_transactions_rejected.reset_index(drop=True)

    logger.info("Data validation complete for user table. %s rows passed checks and %s rows failed and were removed from the population",
                len(df_users), len(df_users_rejected))
    logger.info("Data validation complete for transactions table. %s rows passed checks and %s rows failed and were removed from the population",
                len(df_transactions), len(df_transactions_rejected))


    return df_users, df_users_rejected, df_transactions, df_transactions_rejected

