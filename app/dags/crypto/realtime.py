"""Extract historical data for chosen cryptocurrencies."""
import datetime as dt

from airflow import DAG

from app.operators.storage import (
    FilesToStorageOperator,
    TransformStorageFilesOperator,
    StorageFilesToFirestoreOperator,
)
from app.dags import crypto


GCP_CONN_ID = "google_cloud"
BUCKET_NAME = "stocks_dl"

crypto_dag = DAG(
    dag_id="crypto_realtime",
    schedule_interval="@daily",
    start_date=dt.datetime.today() - dt.timedelta(days=1),
)

download_task = FilesToStorageOperator(
    task_id="download_data",
    dag=crypto_dag,
    files=crypto.REALTIME_PROVIDERS,
    gcp_conn_id=GCP_CONN_ID,
    bucket_name=BUCKET_NAME,
)
transform_task = TransformStorageFilesOperator(
    task_id="transform_data",
    dag=crypto_dag,
    gcp_conn_id=GCP_CONN_ID,
    bucket_name=BUCKET_NAME,
    handlers=crypto.REALTIME_TRANSFORMERS,
)
upload_task = StorageFilesToFirestoreOperator(
    task_id="upload_to_firestore",
    dag=crypto_dag,
    gcp_conn_id=GCP_CONN_ID,
    bucket_name=BUCKET_NAME,
    files=crypto.REALTIME_MASTER_FILES,
    collection_id="crypto",
    key_column="coin",
)
download_task >> transform_task
transform_task >> upload_task
