"""Extract historical data for currencies."""
import datetime as dt

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator,
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
)

from app.operators.storage import FilesToStorageOperator, TransformStorageFilesOperator
from app.operators.bigquery import UpsertGCSToBigQueryOperator
from app.scrapers import currencies
from app.tools import datalake


GCP_CONN_ID = "google_cloud"
BUCKET_NAME = "stocks_dl"
DATASET_ID = "currencies"
ARGS = {"process": "currencies", "dataset": "historical"}
RAW_FILE = datalake.raw(**ARGS, extension="json")
MASTER_FILE = datalake.master(**ARGS, extension="jsonl")
TABLE_ID = "rates"
TEMP_TABLE_ID = TABLE_ID + "_temp{{ ds_nodash }}"
SCHEMA = [
    {"name": "date", "type": "DATE", "mode": "REQUIRED"},
    {"name": "currency", "type": "STRING", "mode": "REQUIRED"},
    {"name": "base", "type": "STRING", "mode": "REQUIRED"},
    {"name": "rate", "type": "FLOAT64", "mode": "REQUIRED"},
]
PARTITIONING = {"type": "MONTH", "field": "date"}
CLUSTER = ["currency"]


currencies_dag = DAG(
    dag_id="currencies_history",
    schedule_interval="@daily",
    start_date=dt.datetime(2021, 8, 1),
)

create_dataset = BigQueryCreateEmptyDatasetOperator(
    task_id="create_dataset",
    dag=currencies_dag,
    gcp_conn_id=GCP_CONN_ID,
    dataset_id=DATASET_ID,
    location="EU",
)
create_table = BigQueryCreateEmptyTableOperator(
    task_id="create_table",
    dag=currencies_dag,
    bigquery_conn_id=GCP_CONN_ID,
    dataset_id=DATASET_ID,
    table_id=TABLE_ID,
    schema_fields=SCHEMA,
    time_partitioning=PARTITIONING,
    cluster_fields=CLUSTER,
    exists_ok=True,
)
download_raw = FilesToStorageOperator(
    task_id="download_data",
    dag=currencies_dag,
    files=[(RAW_FILE, currencies.download_data)],
    gcp_conn_id=GCP_CONN_ID,
    bucket_name=BUCKET_NAME,
)
transform_to_master = TransformStorageFilesOperator(
    task_id="transform_data",
    dag=currencies_dag,
    gcp_conn_id=GCP_CONN_ID,
    bucket_name=BUCKET_NAME,
    handlers=[(RAW_FILE, MASTER_FILE, currencies.parse_data)],
)
upsert_data = UpsertGCSToBigQueryOperator(
    task_id=f"upsert_to_{TABLE_ID}",
    dag=currencies_dag,
    gcp_conn_id=GCP_CONN_ID,
    bucket=BUCKET_NAME,
    source_objects=[MASTER_FILE],
    source_format="NEWLINE_DELIMITED_JSON",
    dataset_id=DATASET_ID,
    table_id=TABLE_ID,
    schema_fields=SCHEMA,
)
verify = BigQueryCheckOperator(
    task_id="verify_data",
    dag=currencies_dag,
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
