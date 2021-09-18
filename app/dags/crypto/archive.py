"""Extract historical data for chosen cryptocurrencies."""
import datetime as dt

from airflow import DAG
from airflow.contrib.operators.gcs_to_bq import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator,
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryInsertJobOperator,
)

from app.operators.storage import FilesToStorageOperator, TransformStorageFilesOperator
from app.dags import crypto
from app.tools import sql


GCP_CONN_ID = "google_cloud"
BUCKET_NAME = "stocks_dl"
DATASET_ID = "cryptocurrencies"
TABLE_ID = "prices"
TEMP_TABLE_ID = TABLE_ID + "_temp{{ ds_nodash }}"
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
    bigquery_conn_id=GCP_CONN_ID,
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
create_temp_table = BigQueryCreateEmptyTableOperator(
    task_id="create_temp_table",
    dag=crypto_dag,
    bigquery_conn_id=GCP_CONN_ID,
    dataset_id=DATASET_ID,
    table_id=TEMP_TABLE_ID,
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

upload_to_temp = GCSToBigQueryOperator(
    task_id=f"upload_temp_{TABLE_ID}",
    dag=crypto_dag,
    bigquery_conn_id=GCP_CONN_ID,
    google_cloud_storage_conn_id=GCP_CONN_ID,
    bucket=BUCKET_NAME,
    source_objects=crypto.ARCHIVE_MASTER_FILES,
    destination_project_dataset_table=f"{DATASET_ID}.{TEMP_TABLE_ID}",
    source_format="NEWLINE_DELIMITED_JSON",
    write_disposition="WRITE_TRUNCATE",
    schema_fields=SCHEMA,
)
replace_in_bq = BigQueryInsertJobOperator(
    dag=crypto_dag,
    task_id=f"replace_{TABLE_ID}",
    gcp_conn_id=GCP_CONN_ID,
    configuration={
        "query": {
            "query": sql.replace_from_temp_bigquery(
                dataset=DATASET_ID,
                dest_table=TABLE_ID,
                temp_table=TEMP_TABLE_ID,
                delete_using="date",
            ),
            "useLegacySql": False,
        }
    },
    location="EU",
)
verify = BigQueryCheckOperator(
    task_id="verify_data",
    dag=crypto_dag,
    gcp_conn_id=GCP_CONN_ID,
    sql='SELECT date FROM {{params.table}} WHERE date = "{{ds}}"',
    params={"table": f"{DATASET_ID}.{TABLE_ID}"},
    use_legacy_sql=False,
)
drop_temp = BigQueryInsertJobOperator(
    task_id=f"drop_temp_{TABLE_ID}",
    gcp_conn_id=GCP_CONN_ID,
    configuration={
        "query": {
            "query": f"DROP TABLE {DATASET_ID}.{TEMP_TABLE_ID};",
            "useLegacySql": False,
        }
    },
    location="EU",
)

create_dataset >> create_table
create_table >> replace_in_bq

create_dataset >> create_temp_table
download_raw >> transform_to_master
create_temp_table >> upload_to_temp
transform_to_master >> upload_to_temp
upload_to_temp >> replace_in_bq
replace_in_bq >> verify
verify >> drop_temp
