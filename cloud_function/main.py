import pandas as pd
from datetime import datetime, timedelta

def update_transactions():
    sql = """
        SELECT *
        FROM `feast-oss.fraud_tutorial.transactions`
    """
    transactions = pd.read_gbq(sql, dialect='standard')
    latest_time = transactions['timestamp'].max()
    datediff = datetime.now() - latest_time.replace(tzinfo=None)
    transactions['timestamp'] = transactions['timestamp'] + datediff
    transactions.to_gbq(destination_table="fraud_tutorial.transactions", project_id="feast-oss", if_exists='replace')

def update_user_features():
    sql = """
        SELECT *
        FROM `feast-oss.fraud_tutorial.user_account_features`
    """
    user_features = pd.read_gbq(sql, dialect='standard')
    user_features['feature_timestamp'] = datetime.now() - timedelta(days=7)
    user_features.to_gbq(destination_table="fraud_tutorial.user_account_features", project_id="feast-oss", if_exists='replace')

def update_user_fraud_features():
    sql = """
        SELECT *
        FROM `feast-oss.fraud_tutorial.user_has_fraudulent_transactions`
    """
    user_has_fraud = pd.read_gbq(sql, dialect='standard')
    latest_time = user_has_fraud['feature_timestamp'].max()
    datediff = datetime.now() - latest_time.replace(tzinfo=None)
    user_has_fraud['feature_timestamp'] = user_has_fraud['feature_timestamp'] + datediff
    user_has_fraud.to_gbq(destination_table="fraud_tutorial.user_has_fraudulent_transactions", project_id="feast-oss", if_exists='replace')

def main(data, context):
    update_transactions()
    update_user_features()
    update_user_fraud_features()

main(1, 1)