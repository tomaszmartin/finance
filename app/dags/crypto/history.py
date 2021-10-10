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
from app.dags.crypto import config


with DAG(
    dag_id="crypto_history",
    description="Scrapes historical prices of some popular cryptocurrencies.",
    schedule_interval="@daily",
    start_date=dt.datetime.today() - dt.timedelta(days=3),
) as dag:
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
        cluster_fields=["coin"],
        time_partitioning={"type": "MONTH", "field": "date"},
        exists_ok=True,
    )
    download_raw = FilesToStorageOperator(
        task_id="download_coins",
        files=config.ARCHIVE_PROVIDERS,
        gcp_conn_id=config.GCP_CONN_ID,
        bucket_name=config.BUCKET_NAME,
    )
    transform_to_master = TransformStorageFilesOperator(
        task_id="transform_coins",
        gcp_conn_id=config.GCP_CONN_ID,
        bucket_name=config.BUCKET_NAME,
        handlers=config.ARCHIVE_TRANSFORMERS,
    )
    upsert_data = UpsertGCSToBigQueryOperator(
        task_id=f"upsert_to_{config.HISTORY_TABLE_ID}",
        gcp_conn_id=config.GCP_CONN_ID,
        bucket_name=config.BUCKET_NAME,
        source_objects=config.ARCHIVE_MASTER_FILES,
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

    # pylint: disable=pointless-statement
    (
        create_dataset
        >> create_table
        >> download_raw
        >> transform_to_master
        >> upsert_data
        >> verify
    )
