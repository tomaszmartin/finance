import datetime as dt
from typing import Any, Dict, List
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.contrib.hooks.bigquery_hook import BigQueryHook

from app.scrapers import stocks


def generate_name(instrument: str, execution_date: dt.datetime):
    return f"stocks/raw/{instrument}/{execution_date.year}/{execution_date.strftime('%Y-%m-%d')}.html"


def get_table_schema(instrument: str) -> List[Dict[str, str]]:
    schemas = {
        "equities": [
            {"name": "date", "type": "DATETIME", "mode": "REQUIRED"},
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
    }
    return schemas[instrument]


def create_table(dataset_id: str, instrument: str) -> None:
    bq_hook = BigQueryHook(gcp_conn_id="google_cloud")
    bq_hook.create_empty_dataset(
        dataset_id=dataset_id,
        exists_ok=True,
        location="EU",
    )
    bq_hook.create_empty_table(
        dataset_id=dataset_id,
        table_id=instrument,
        schema_fields=get_table_schema(instrument),
        exists_ok=True,
    )


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
    storage_hook = GoogleCloudStorageHook(google_cloud_storage_conn_id="google_cloud")
    storage_hook.upload(bucket_name, object_name=name, data=stocks_data)


def parse(
    dataset_id: str,
    instrument: str,
    execution_date: dt.datetime,
    bucket_name: str,
    **context: Dict[str, Any],
) -> None:
    logger = logging.getLogger("airflow.task")
    logger.info("Downloading %s data for %s", instrument, execution_date)
    name = generate_name(instrument, execution_date)
    storage_hook = GoogleCloudStorageHook(google_cloud_storage_conn_id="google_cloud")
    stocks_data = storage_hook.download(object_name=name, bucket_name=bucket_name)
    parsed_data = stocks.parse_stocks(stocks_data, execution_date)
    bq_hook = BigQueryHook(gcp_conn_id="google_cloud")
    bq_hook.insert_all(dataset_id=dataset_id, table_id=instrument, rows=parsed_data)


stocks_dag = DAG(
    dag_id="stocks_etl",
    schedule_interval="@daily",
    start_date=dt.datetime(2021, 6, 1),
)

download_equities = PythonOperator(
    task_id="download_equities",
    dag=stocks_dag,
    python_callable=download,
    op_kwargs={"instrument": "equities", "bucket_name": "sandbox_data_lake"},
)

create_equities_table = PythonOperator(
    task_id="create_equities_table",
    dag=stocks_dag,
    python_callable=create_table,
    op_kwargs={"instrument": "equities", "dataset_id": "stocks"},
)

equities_to_bigquery = PythonOperator(
    task_id="equities_to_bigquery",
    dag=stocks_dag,
    python_callable=parse,
    op_kwargs={
        "dataset_id": "stocks",
        "instrument": "equities",
        "bucket_name": "sandbox_data_lake",
    },
)

download_indices = PythonOperator(
    task_id="download_indices",
    dag=stocks_dag,
    python_callable=download,
    op_kwargs={"instrument": "indices", "bucket_name": "sandbox_data_lake"},
)

download_equities >> create_equities_table
create_equities_table >> equities_to_bigquery
