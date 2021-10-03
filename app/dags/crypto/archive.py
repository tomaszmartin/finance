"""Extract historical data for chosen cryptocurrencies."""
import datetime as dt

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator,
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
)

from app.operators.storage import FilesToStorageOperator, TransformStorageFilesOperator
from app.operators.bigquery import UpsertGCSToBigQueryOperator
from app.dags import crypto


GCP_CONN_ID = "google_cloud"
BUCKET_NAME = "stocks_dl"
DATASET_ID = "cryptocurrencies"
TABLE_ID = "prices"
SCHEMA = [
    {"name": "date", "type": "DATE", "mode": "REQUIRED"},
    {"name": "coin", "type": "STRING", "mode": "REQUIRED"},
    {"name": "base", "type": "STRING", "mode": "REQUIRED"},
    {"name": "open", "type": "FLOAT64", "mode": "REQUIRED"},
    {"name": "high", "type": "FLOAT64", "mode": "REQUIRED"},
    {"name": "low", "type": "FLOAT64", "mode": "REQUIRED"},
    {"name": "close", "type": "FLOAT64", "mode": "REQUIRED"},
]
CLUSTER = ["coin"]
PARTITIONING = {"type": "MONTH", "field": "date"}

crypto_dag = DAG(
    dag_id="crypto_history",
    schedule_interval="@daily",
    start_date=dt.datetime(2021, 8, 1),
)

create_dataset = BigQueryCreateEmptyDatasetOperator(
    task_id="create_dataset",
    dag=crypto_dag,
    gcp_conn_id=GCP_CONN_ID,
    dataset_id=DATASET_ID,
    location="EU",
)
create_table = BigQueryCreateEmptyTableOperator(
    task_id="create_table",
    dag=crypto_dag,
    bigquery_conn_id=GCP_CONN_ID,
    dataset_id=DATASET_ID,
    table_id=TABLE_ID,
    schema_fields=SCHEMA,
    cluster_fields=CLUSTER,
    time_partitioning=PARTITIONING,
    exists_ok=True,
)
download_raw = FilesToStorageOperator(
    task_id="download_coins",
    dag=crypto_dag,
    files=crypto.ARCHIVE_PROVIDERS,
    gcp_conn_id=GCP_CONN_ID,
    bucket_name=BUCKET_NAME,
)
transform_to_master = TransformStorageFilesOperator(
    task_id="transform_coins",
    dag=crypto_dag,
    gcp_conn_id=GCP_CONN_ID,
    bucket_name=BUCKET_NAME,
    handlers=crypto.ARCHIVE_TRANSFORMERS,
)
upsert_data = UpsertGCSToBigQueryOperator(
    task_id=f"upsert_to_{TABLE_ID}",
    dag=crypto_dag,
    gcp_conn_id=GCP_CONN_ID,
    bucket=BUCKET_NAME,
    source_objects=crypto.ARCHIVE_MASTER_FILES,
    source_format="NEWLINE_DELIMITED_JSON",
    dataset_id=DATASET_ID,
    table_id=TABLE_ID,
    schema_fields=SCHEMA,
)
verify = BigQueryCheckOperator(
    task_id="verify_data",
    dag=crypto_dag,
    gcp_conn_id=GCP_CONN_ID,
    sql='SELECT date FROM {{params.table}} WHERE date = "{{ds}}"',
    params={"table": f"{DATASET_ID}.{TABLE_ID}"},
    use_legacy_sql=False,
)

(
    create_dataset
    >> create_table
    >> download_raw
    >> transform_to_master
    >> upsert_data
    >> verify
)
