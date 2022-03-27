"""Extracts historical data from GPW Polish Stock Exchange."""
import datetime as dt
from functools import partial

from airflow import DAG
from airflow.operators.python import ShortCircuitOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator,
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
)

from app.dags.gpw import config
from app.operators.bigquery import UpsertGCSToBigQueryOperator
from app.operators.storage import FileToGCSOperator, TransformGCSFileOperator
from app.scrapers.stocks import prices
from app.tools import datalake, dates

with DAG(
    dag_id="gpw_history",
    description="Scrapes historical prices of equities and indices on GPW.",
    schedule_interval="@daily",
    start_date=dt.datetime(2021, 11, 1),
    catchup=False,
) as dag:
    check_holidays = ShortCircuitOperator(
        task_id="check_if_workday",
        python_callable=dates.is_workday,
    )
    for instrument in ["equities", "indices"]:
        PARAMS = {"process": "gpw", "dataset": "historical", "prefix": instrument}
        RAW_FILE = datalake.raw(extension="html", **PARAMS)
        MASTER_FILE = datalake.master(extension="jsonl", **PARAMS)
        TABLE_ID = config.TABLES[instrument]
        SCHEMA = config.SCHEMAS[instrument]

        create_dataset = BigQueryCreateEmptyDatasetOperator(
            task_id=f"create_{instrument}_dataset",
            gcp_conn_id=config.GCP_CONN_ID,
            dataset_id=config.DATASET_ID,
            location="EU",
        )
        create_table = BigQueryCreateEmptyTableOperator(
            task_id=f"create_{TABLE_ID}_table",
            bigquery_conn_id=config.GCP_CONN_ID,
            dataset_id=config.DATASET_ID,
            table_id=TABLE_ID,
            schema_fields=SCHEMA,
            cluster_fields=["isin_code"],
            time_partitioning={"type": "MONTH", "field": "date"},
            exists_ok=True,
        )
        download_raw = FileToGCSOperator(
            task_id=f"download_{instrument}",
            gcp_conn_id=config.GCP_CONN_ID,
            bucket_name=config.BUCKET_NAME,
            object_name=RAW_FILE,
            object_provider=partial(prices.get_archive, instrument),
        )
        transform_to_master = TransformGCSFileOperator(
            task_id=f"transform_{instrument}",
            gcp_conn_id=config.GCP_CONN_ID,
            bucket_name=config.BUCKET_NAME,
            source_object=RAW_FILE,
            destination_object=MASTER_FILE,
            transformation=prices.parse_archive,
        )
        upsert_data = UpsertGCSToBigQueryOperator(
            task_id=f"upsert_to_{TABLE_ID}",
            gcp_conn_id=config.GCP_CONN_ID,
            bucket_name=config.BUCKET_NAME,
            source_objects=[MASTER_FILE],
            source_format="NEWLINE_DELIMITED_JSON",
            dataset_id=config.DATASET_ID,
            table_id=TABLE_ID,
            schema_fields=SCHEMA,
        )
        verify = BigQueryCheckOperator(
            task_id=f"verify_{instrument}",
            gcp_conn_id=config.GCP_CONN_ID,
            sql='SELECT date FROM {{params.table}} WHERE date = "{{ds}}"',
            params={"table": f"{config.DATASET_ID}.{TABLE_ID}"},
            use_legacy_sql=False,
        )

        # pylint: disable=pointless-statement
        (
            check_holidays
            >> create_dataset
            >> create_table
            >> download_raw
            >> transform_to_master
            >> upsert_data
            >> verify
        )
