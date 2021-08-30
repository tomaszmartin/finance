"""Extract historical data for chosen cryptocurrencies."""
import datetime as dt
from functools import partial

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator,
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
)

from app.operators.scraping import FilesToBucketOperator, BucketFilesToBigQueryOperator
from app.scrapers import crypto


GCP_CONN_ID = "google_cloud"
BUCKET_NAME = "sandbox_data_lake"
DATASET_ID = "crypto"
FILE_PROVIDERS = {
    f"crypto/archive/{coin}" + "{{ds}}.json": partial(crypto.download_data, coin)
    for coin in crypto.COINS
}
FILE_PARSERS = {
    f"crypto/archive/{coin}" + "{{ds}}.json": partial(crypto.parse_data, coin=coin)
    for coin in crypto.COINS
}
TABLE = "crypto"

crypto_dag = DAG(
    dag_id="crypto_archive",
    schedule_interval="@daily",
    start_date=dt.datetime.today() - dt.timedelta(days=1),
)

download_task = FilesToBucketOperator(
    task_id="download_coins",
    dag=crypto_dag,
    file_providers=FILE_PROVIDERS,
    gcp_conn_id=GCP_CONN_ID,
    bucket_name=BUCKET_NAME,
)
create_dataset = BigQueryCreateEmptyDatasetOperator(
    task_id="create_dataset",
    dag=crypto_dag,
    bigquery_conn_id=GCP_CONN_ID,
    dataset_id=DATASET_ID,
    location="EU",
)
create_table = BigQueryCreateEmptyTableOperator(
    task_id="create_table",
    dag=crypto_dag,
    bigquery_conn_id=GCP_CONN_ID,
    dataset_id=DATASET_ID,
    table_id=TABLE,
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
to_bigquery_task = BucketFilesToBigQueryOperator(
    task_id="coins_to_bigquery",
    dag=crypto_dag,
    file_parsers=FILE_PARSERS,
    gcp_conn_id=GCP_CONN_ID,
    bucket_name=BUCKET_NAME,
    dataset_id=DATASET_ID,
    table_id=TABLE,
)
verify_data = BigQueryCheckOperator(
    task_id="verify_data",
    dag=crypto_dag,
    gcp_conn_id=GCP_CONN_ID,
    sql='SELECT date FROM {{params.table}} WHERE date = "{{ds}}"',
    params={"table": f"{DATASET_ID}.{TABLE}"},
    use_legacy_sql=False,
)

download_task >> to_bigquery_task
create_dataset >> create_table
create_table >> to_bigquery_task
to_bigquery_task >> verify_data
