import datetime as dt
from typing import Any, Dict
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator

from app.scrapers import stocks


def generate_name(instrument: str, execution_date: dt.datetime):
    directory = f"stocks/raw/{instrument}/{execution_date.year}/"
    filename = f"{execution_date.isoformat()}.html"
    return f"{directory}{filename}"


def download(
    instrument: str,
    execution_date: dt.datetime,
    bucket_name: str,
    **context: Dict[str, Any],
) -> None:
    logger = logging.getLogger("airflow.task")
    logger.info("Downloading %s data for %s", instrument, execution_date)
    name = generate_name(instrument, execution_date)
    stocks_data = stocks.get_stocks(instrument, execution_date)
    storage_hook = GCSHook(google_cloud_storage_conn_id="google_cloud")
    storage_hook.upload(bucket_name, object_name=name, data=stocks_data)


def create_table(
    dataset_id: str,
    table_id: str,
) -> None:
    bq_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID)
    bq_hook.create_empty_dataset(
        dataset_id=dataset_id,
        exists_ok=True,
        location="EU",
    )
    bq_hook.create_empty_table(
        dataset_id=dataset_id,
        table_id=table_id,
        schema_fields=[
            {"name": "datetime", "type": "DATETIME", "mode": "REQUIRED"},
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
            "field": "datetime",
        },
    )


def to_bigquery(
    dataset_id: str,
    instrument: str,
    execution_date: dt.datetime,
    bucket_name: str,
    **context: Dict[str, Any],
) -> None:
    logger = logging.getLogger("airflow.task")
    logger.info("Downloading %s data for %s", instrument, execution_date)
    name = generate_name(instrument, execution_date)
    storage_hook = GCSHook(google_cloud_storage_conn_id=GCP_CONN_ID)
    stocks_data = storage_hook.download(object_name=name, bucket_name=bucket_name)
    parsed_data = stocks.parse_stocks(stocks_data, execution_date)
    bq_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID)
    bq_hook.insert_all(dataset_id=dataset_id, table_id=instrument, rows=parsed_data)


GCP_CONN_ID = "google_cloud"
DATASET_ID = "stocks"

stocks_dag = DAG(
    dag_id="download_stocks",
    # Run on workdays at 17:15
    schedule_interval="15 17 * * 1-5",
    start_date=dt.datetime(2021, 6, 1),
)

for instrument in ["equities", "indices"]:
    download_task = PythonOperator(
        task_id=f"download_{instrument}",
        dag=stocks_dag,
        python_callable=download,
        op_kwargs={"instrument": instrument, "bucket_name": "sandbox_data_lake"},
    )
    create_table_task = PythonOperator(
        task_id=f"create_{instrument}_table",
        dag=stocks_dag,
        python_callable=create_table,
        op_kwargs={
            "dataset_id": DATASET_ID,
            "table_id": instrument,
        },
    )
    to_bigquery_task = PythonOperator(
        task_id=f"{instrument}_to_bigquery",
        dag=stocks_dag,
        python_callable=to_bigquery,
        op_kwargs={
            "dataset_id": DATASET_ID,
            "instrument": instrument,
            "bucket_name": "sandbox_data_lake",
        },
    )
    verify_data = BigQueryCheckOperator(
        task_id=f"verify_{instrument}",
        dag=stocks_dag,
        gcp_conn_id=GCP_CONN_ID,
        sql='SELECT datetime FROM {{params.table}} WHERE DATE(datetime) = "{{ds}}"',
        params={"table": f"{DATASET_ID}.{instrument}"},
        use_legacy_sql=False,
    )
    download_task >> to_bigquery_task
    create_table_task >> to_bigquery_task
    to_bigquery_task >> verify_data
