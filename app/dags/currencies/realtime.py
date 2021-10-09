"""Extract historical data for currencies."""
import datetime as dt
from functools import partial

from airflow import DAG

from app.operators.storage import (
    FilesToStorageOperator,
    TransformStorageFilesOperator,
    StorageFilesToFirestoreOperator,
)
from app.scrapers import currencies
from app.tools import datalake
from app.dags.currencies import config


ARGS = {"process": "currencies", "dataset": "realtime", "extension": "json"}
RAW_FILE = datalake.raw(**ARGS, with_timestamp=True)
MASTER_FILE = datalake.master(**ARGS, with_timestamp=True)

with DAG(
    dag_id="currencies_realtime",
    schedule_interval="@daily",
    start_date=dt.datetime.today() - dt.timedelta(days=1),
) as dag:
    download_task = FilesToStorageOperator(
        task_id="download_data",
        files=[(RAW_FILE, partial(currencies.download_data, realtime=True))],
        gcp_conn_id=config.GCP_CONN_ID,
        bucket_name=config.BUCKET_NAME,
    )
    transform_task = TransformStorageFilesOperator(
        task_id="transform_data",
        gcp_conn_id=config.GCP_CONN_ID,
        bucket_name=config.BUCKET_NAME,
        handlers=[(RAW_FILE, MASTER_FILE, currencies.parse_data)],
    )
    upload_task = StorageFilesToFirestoreOperator(
        task_id="upload_data",
        gcp_conn_id=config.GCP_CONN_ID,
        bucket_name=config.BUCKET_NAME,
        files=[MASTER_FILE],
        collection_id="currencies",
        key_column="currency",
    )

    (download_task >> transform_task >> upload_task)
