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
from app.dags.currencies import config


with DAG(
    dag_id="currencies_history",
    description="Scrapes historical prices of currencies.",
    schedule_interval="@daily",
    start_date=dt.datetime.today() - dt.timedelta(days=3),
) as dag:
    ARGS = {"process": "currencies", "dataset": "historical"}
    RAW_FILE = datalake.raw(**ARGS, extension="json")
    MASTER_FILE = datalake.master(**ARGS, extension="jsonl")

    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset",
        gcp_conn_id=config.GCP_CONN_ID,
        dataset_id=config.DATASET_ID,
        location="EU",
    )
    create_table = BigQueryCreateEmptyTableOperator(
        task_id="create_table",
        bigquery_conn_id=config.GCP_CONN_ID,
        dataset_id=config.DATASET_ID,
        table_id=config.HISTORY_TABLE_ID,
        schema_fields=config.HISTORY_SCHEMA,
        time_partitioning={"type": "MONTH", "field": "date"},
        cluster_fields=["currency"],
        exists_ok=True,
    )
    download_raw = FilesToStorageOperator(
        task_id="download_data",
        files=[(RAW_FILE, currencies.download_data)],
        gcp_conn_id=config.GCP_CONN_ID,
        bucket_name=config.BUCKET_NAME,
    )
    transform_to_master = TransformStorageFilesOperator(
        task_id="transform_data",
        gcp_conn_id=config.GCP_CONN_ID,
        bucket_name=config.BUCKET_NAME,
        handlers=[(RAW_FILE, MASTER_FILE, currencies.parse_data)],
    )
    upsert_data = UpsertGCSToBigQueryOperator(
        task_id=f"upsert_to_{config.HISTORY_TABLE_ID}",
        gcp_conn_id=config.GCP_CONN_ID,
        bucket_name=config.BUCKET_NAME,
        source_objects=[MASTER_FILE],
        source_format="NEWLINE_DELIMITED_JSON",
        dataset_id=config.DATASET_ID,
        table_id=config.HISTORY_TABLE_ID,
        schema_fields=config.HISTORY_SCHEMA,
    )
    verify = BigQueryCheckOperator(
        task_id="verify_data",
        gcp_conn_id=config.GCP_CONN_ID,
        sql='SELECT date FROM {{params.table}} WHERE date = "{{ds}}"',
        params={"table": f"{config.DATASET_ID}.{config.HISTORY_TABLE_ID}"},
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
