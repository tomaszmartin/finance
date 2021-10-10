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
from app.dags.gpw import config


with DAG(
    dag_id="gpw_realtime",
    schedule_interval="0 9-17 * * 1-5",
    start_date=dt.datetime.today() - dt.timedelta(days=1),
) as dag:
    for instrument in ["equities", "indices"]:
        PARAMS = {"process": "gpw", "dataset": "realtime", "prefix": instrument}
        RAW_FILE = datalake.raw(extension="html", **PARAMS)
        MASTER_FILE = datalake.master(extension="json", **PARAMS)
        download_task = FilesToStorageOperator(
            task_id=f"download_{instrument}",
            files=[(RAW_FILE, partial(prices.get_realtime, instrument))],
            gcp_conn_id=config.GCP_CONN_ID,
            bucket_name=config.BUCKET_NAME,
        )
        transform_task = TransformStorageFilesOperator(
            task_id=f"transform_{instrument}",
            gcp_conn_id=config.GCP_CONN_ID,
            bucket_name=config.BUCKET_NAME,
            handlers=[(RAW_FILE, MASTER_FILE, prices.parse_realtime)],
        )
        upload_task = StorageFilesToFirestoreOperator(
            task_id=f"{instrument}_to_firestore",
            gcp_conn_id=config.GCP_CONN_ID,
            bucket_name=config.BUCKET_NAME,
            files=[MASTER_FILE],
            collection_id=instrument,
            key_column="isin_code",
        )

        # pylint: disable=pointless-statement
        (download_task >> transform_task >> upload_task)
