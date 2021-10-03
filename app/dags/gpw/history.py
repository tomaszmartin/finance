"""Extracts historical data from GPW Polish Stock Exchange."""
import datetime as dt
from functools import partial

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator,
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
)

from app.scrapers.stocks import prices
from app.operators.bigquery import UpsertGCSToBigQueryOperator
from app.operators.storage import (
    FilesToStorageOperator,
    TransformStorageFilesOperator,
)
from app.tools import datalake


GCP_CONN_ID = "google_cloud"
DATASET_ID = "stocks"
BUCKET_NAME = "stocks_dl"
SCHEMA = [
    {"name": "date", "type": "DATE", "mode": "REQUIRED"},
    {"name": "name", "type": "STRING", "mode": "REQUIRED"},
    {"name": "isin_code", "type": "STRING", "mode": "REQUIRED"},
    {"name": "currency", "type": "STRING", "mode": "REQUIRED"},
    {"name": "opening_price", "type": "FLOAT64", "mode": "REQUIRED"},
    {"name": "closing_price", "type": "FLOAT64", "mode": "REQUIRED"},
    {"name": "minimum_price", "type": "FLOAT64", "mode": "REQUIRED"},
    {"name": "maximum_price", "type": "FLOAT64", "mode": "REQUIRED"},
    {"name": "number_of_transactions", "type": "FLOAT64", "mode": "REQUIRED"},
    {"name": "trade_volume", "type": "FLOAT64", "mode": "REQUIRED"},
    {"name": "turnover_value", "type": "FLOAT64", "mode": "REQUIRED"},
]

archive_dag = DAG(
    dag_id="gpw_history",
    description="Scrapes historical prices of equities and indices on GPW.",
    schedule_interval="15 17 * * 1-5",
    start_date=dt.datetime.today() - dt.timedelta(days=3),
)

for instrument in ["equities", "indices"]:
    PARAMS = {"process": "gpw", "dataset": "historical", "prefix": instrument}
    RAW_FILE = datalake.raw(extension="html", **PARAMS)
    MASTER_FILE = datalake.master(extension="jsonl", **PARAMS)
    TABLE_ID = instrument

    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id=f"create_{instrument}_dataset",
        dag=archive_dag,
        gcp_conn_id=GCP_CONN_ID,
        dataset_id=DATASET_ID,
        location="EU",
    )
    create_table = BigQueryCreateEmptyTableOperator(
        task_id=f"create_{TABLE_ID}_table",
        dag=archive_dag,
        bigquery_conn_id=GCP_CONN_ID,
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
        schema_fields=SCHEMA,
        cluster_fields=["isin_code"],
        time_partitioning={"type": "MONTH", "field": "date"},
        exists_ok=True,
    )
    download_raw = FilesToStorageOperator(
        task_id=f"download_{instrument}",
        dag=archive_dag,
        gcp_conn_id=GCP_CONN_ID,
        bucket_name=BUCKET_NAME,
        files=[(RAW_FILE, partial(prices.get_archive, instrument))],
    )
    transform_to_master = TransformStorageFilesOperator(
        task_id=f"transform_{instrument}",
        dag=archive_dag,
        gcp_conn_id=GCP_CONN_ID,
        bucket_name=BUCKET_NAME,
        handlers=[(RAW_FILE, MASTER_FILE, prices.parse_archive)],
    )
    upsert_data = UpsertGCSToBigQueryOperator(
        task_id=f"upsert_to_{TABLE_ID}",
        dag=archive_dag,
        gcp_conn_id=GCP_CONN_ID,
        bucket=BUCKET_NAME,
        source_objects=[MASTER_FILE],
        source_format="NEWLINE_DELIMITED_JSON",
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
        schema_fields=SCHEMA,
    )
    verify = BigQueryCheckOperator(
        task_id=f"verify_{instrument}",
        dag=archive_dag,
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
