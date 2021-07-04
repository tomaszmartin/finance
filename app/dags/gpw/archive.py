import datetime as dt
from functools import partial
from typing import Any, Dict
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator

from app.scrapers import stocks
from app.operators.scraping import (
    FileToBucketOperator,
    BucketFileToBigQueryOperator,
    BucketFileToFirestoreOperator,
)


def create_table(
    dataset_id: str,
    table_id: str,
    gcp_conn_id: str,
) -> None:
    bq_hook = BigQueryHook(gcp_conn_id=gcp_conn_id)
    bq_hook.create_empty_dataset(
        dataset_id=dataset_id,
        exists_ok=True,
        location="EU",
    )
    bq_hook.create_empty_table(
        dataset_id=dataset_id,
        table_id=table_id,
        schema_fields=[
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
        ],
        exists_ok=True,
        time_partitioning={
            "type": "MONTH",
            "field": "date",
        },
    )


GCP_CONN_ID = "google_cloud"
DATASET_ID = "stocks"
BUCKET_NAME = "sandbox_data_lake"

archive_dag = DAG(
    dag_id="stocks_archive_etl",
    # Run on workdays at 17:15
    schedule_interval="15 17 * * 1-5",
    start_date=dt.datetime.today(),
)

for instrument in ["equities", "indices"]:
    object_name = f"stocks/archive/{instrument}/"
    object_name += "{{execution_date.year}}/{{ds}}.html"
    download_task = FileToBucketOperator(
        task_id=f"download_{instrument}",
        dag=archive_dag,
        file_provider=partial(stocks.get_archive, instrument),
        gcp_conn_id=GCP_CONN_ID,
        bucket_name=BUCKET_NAME,
        object_name=object_name,
    )
    create_table_task = PythonOperator(
        task_id=f"create_{instrument}_table",
        dag=archive_dag,
        python_callable=create_table,
        op_kwargs={
            "gcp_conn_id": GCP_CONN_ID,
            "dataset_id": DATASET_ID,
            "table_id": instrument,
        },
    )
    to_bigquery_task = BucketFileToBigQueryOperator(
        task_id=f"{instrument}_to_bigquery",
        dag=archive_dag,
        parse_func=stocks.parse_archive,
        gcp_conn_id=GCP_CONN_ID,
        bucket_name=BUCKET_NAME,
        object_name=object_name,
        dataset_id=DATASET_ID,
        table_id=instrument,
    )
    verify_data = BigQueryCheckOperator(
        task_id=f"verify_{instrument}",
        dag=archive_dag,
        gcp_conn_id=GCP_CONN_ID,
        sql='SELECT date FROM {{params.table}} WHERE date = "{{ds}}"',
        params={"table": f"{DATASET_ID}.{instrument}"},
        use_legacy_sql=False,
    )
    download_task >> to_bigquery_task
    create_table_task >> to_bigquery_task
    to_bigquery_task >> verify_data
