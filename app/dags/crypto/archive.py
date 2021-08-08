"""Extract historical data for chosen cryptocurrencies."""
import datetime as dt
from functools import partial

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator,
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
)

from app.operators.scraping import FileToBucketOperator, BucketFileToBigQueryOperator
from app.scrapers import crypto


GCP_CONN_ID = "google_cloud"
BUCKET_NAME = "sandbox_data_lake"
DATASET_ID = "crypto"
coins = [
    "BTC",
    "ETH",
    "LTC",
    "XRP",
    "ETC",
    "XLM",
    "DASH",
    "BTT",
]

crypto_dag = DAG(
    dag_id="crypto_archive",
    schedule_interval="@daily",
    start_date=dt.datetime.today() - dt.timedelta(days=1),
)
for curr_coin in coins:
    COIN = curr_coin.lower()
    OBJ_NAME = f"crypto/archive/{COIN}" + "{{ds}}.json"
    download_task = FileToBucketOperator(
        task_id=f"download_{COIN}",
        dag=crypto_dag,
        file_provider=partial(crypto.download_data, COIN),
        gcp_conn_id=GCP_CONN_ID,
        bucket_name=BUCKET_NAME,
        object_name=OBJ_NAME,
    )
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id=f"create_{COIN}_dataset",
        dag=crypto_dag,
        bigquery_conn_id=GCP_CONN_ID,
        dataset_id=DATASET_ID,
        location="EU",
    )
    create_table = BigQueryCreateEmptyTableOperator(
        task_id=f"create_{COIN}_table",
        dag=crypto_dag,
        bigquery_conn_id=GCP_CONN_ID,
        dataset_id=DATASET_ID,
        table_id=COIN,
        schema_fields=[
            {"name": "date", "type": "DATE", "mode": "REQUIRED"},
            {"name": "coin", "type": "STRING", "mode": "REQUIRED"},
            {"name": "base", "type": "STRING", "mode": "REQUIRED"},
            {"name": "open", "type": "FLOAT64", "mode": "REQUIRED"},
            {"name": "high", "type": "FLOAT64", "mode": "REQUIRED"},
            {"name": "low", "type": "FLOAT64", "mode": "REQUIRED"},
            {"name": "close", "type": "FLOAT64", "mode": "REQUIRED"},
        ],
        time_partitioning={
            "type": "MONTH",
            "field": "date",
        },
        exists_ok=True,
    )
    to_bigquery_task = BucketFileToBigQueryOperator(
        task_id=f"{COIN}_to_bigquery",
        dag=crypto_dag,
        parse_func=partial(crypto.parse_data, coin=COIN),
        gcp_conn_id=GCP_CONN_ID,
        bucket_name=BUCKET_NAME,
        object_name=OBJ_NAME,
        dataset_id=DATASET_ID,
        table_id=COIN,
    )
    verify_data = BigQueryCheckOperator(
        task_id=f"verify_{COIN}",
        dag=crypto_dag,
        gcp_conn_id=GCP_CONN_ID,
        sql='SELECT date FROM {{params.table}} WHERE date = "{{ds}}"',
        params={"table": f"{DATASET_ID}.{COIN}"},
        use_legacy_sql=False,
    )
    download_task >> to_bigquery_task
    create_dataset >> create_table
    create_table >> to_bigquery_task
    to_bigquery_task >> verify_data
