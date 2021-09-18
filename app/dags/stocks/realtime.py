"""Extracts current (realtime) data from GPW Polish Stock Exchange."""
import datetime as dt
from functools import partial

from airflow import DAG

from app.scrapers.stocks import prices
from app.operators.storage import (
    FilesToStorageOperator,
    TransformStorageFilesOperator,
    StorageFilesToFirestoreOperator,
)
from app.tools import datalake


GCP_CONN_ID = "google_cloud"
BUCKET_NAME = "stocks_dl"

realtime_dag = DAG(
    dag_id="stocks_realtime",
    schedule_interval="0 9-17 * * 1-5",
    start_date=dt.datetime.today(),
)

for instrument in ["equities", "indices"]:
    PARAMS = {"process": "gpw", "dataset": "realtime_{instrument}"}
    RAW_FILE = datalake.raw(extension="html", **PARAMS)
    MASTER_FILE = datalake.master(extension="json", **PARAMS)
    download_task = FilesToStorageOperator(
        task_id=f"download_{instrument}",
        dag=realtime_dag,
        files=[(RAW_FILE, partial(prices.get_current, instrument))],
        gcp_conn_id=GCP_CONN_ID,
        bucket_name=BUCKET_NAME,
    )
    transform_task = TransformStorageFilesOperator(
        task_id=f"transform_{instrument}",
        dag=realtime_dag,
        gcp_conn_id=GCP_CONN_ID,
        bucket_name=BUCKET_NAME,
        handlers=[(RAW_FILE, MASTER_FILE, partial(prices.get_current, instrument))],
    )
    upload_task = StorageFilesToFirestoreOperator(
        task_id=f"{instrument}_to_firestore",
        dag=realtime_dag,
        gcp_conn_id=GCP_CONN_ID,
        bucket_name=BUCKET_NAME,
        files=[MASTER_FILE],
        collection_id=instrument,
        key_column="isin_code",
    )
    download_task >> transform_task
    transform_task >> upload_task
