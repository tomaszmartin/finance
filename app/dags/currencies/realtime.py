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


GCP_CONN_ID = "google_cloud"
BUCKET_NAME = "stocks_dl"
ARGS = {"process": "currencies", "dataset": "realtime", "extension": "json"}
RAW_FILE = datalake.raw(**ARGS, with_timestamp=True)
MASTER_FILE = datalake.master(**ARGS, with_timestamp=True)
TABLE_ID = "rates"

currencies_dag = DAG(
    dag_id="currencies_realtime",
    schedule_interval="@daily",
    start_date=dt.datetime.today() - dt.timedelta(days=1),
)

download_task = FilesToStorageOperator(
    task_id="download_data",
    dag=currencies_dag,
    files=[(RAW_FILE, partial(currencies.download_data, realtime=True))],
    gcp_conn_id=GCP_CONN_ID,
    bucket_name=BUCKET_NAME,
)
transform_task = TransformStorageFilesOperator(
    task_id="transform_data",
    dag=currencies_dag,
    gcp_conn_id=GCP_CONN_ID,
    bucket_name=BUCKET_NAME,
    handlers=[(RAW_FILE, MASTER_FILE, currencies.parse_data)],
)
upload_task = StorageFilesToFirestoreOperator(
    task_id="upload_data",
    dag=currencies_dag,
    gcp_conn_id=GCP_CONN_ID,
    bucket_name=BUCKET_NAME,
    files=[MASTER_FILE],
    collection_id="currencies",
    key_column="currency",
)
download_task >> transform_task
transform_task >> upload_task
