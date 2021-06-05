import datetime as dt
from typing import Dict, Any
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook

from app.scrapers import stocks


def download(
    instrument: str, execution_date: dt.date, bucket_name, **context: Dict[str, Any]
) -> None:
    logger = logging.getLogger("airflow.task")
    logger.info("Downloading %s data for %s", instrument, execution_date)
    name = f"stocks/raw/{instrument}/{execution_date.year}/{execution_date.strftime('%Y-%m-%d')}.html"
    stocks_data = stocks.get_stocks(instrument, execution_date)
    storage_hook = GoogleCloudStorageHook(google_cloud_storage_conn_id="google_cloud")
    storage_hook.upload(bucket_name, object_name=name, data=stocks_data)


stocks_dag = DAG(
    dag_id="stocks_to_gcs",
    schedule_interval="@daily",
    start_date=dt.datetime(2021, 1, 1),
)

start_task = DummyOperator(task_id="start", dag=stocks_dag)

download_equities = PythonOperator(
    task_id="download_equities",
    dag=stocks_dag,
    python_callable=download,
    op_kwargs={"instrument": "equities", "bucket_name": "sandbox_data_lake"},
)

download_indices = PythonOperator(
    task_id="download_indices",
    dag=stocks_dag,
    python_callable=download,
    op_kwargs={"instrument": "indices", "bucket_name": "sandbox_data_lake"},
)

start_task >> download_equities
start_task >> download_indices
