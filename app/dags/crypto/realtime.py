"""Extract historical data for chosen cryptocurrencies."""
import datetime as dt

from airflow import DAG

from app.operators.storage import (
    FilesToStorageOperator,
    TransformStorageFilesOperator,
    StorageFilesToFirestoreOperator,
)
from app.dags.crypto import config


with DAG(
    dag_id="crypto_realtime",
    schedule_interval="@daily",
    start_date=dt.datetime.today() - dt.timedelta(days=1),
) as dag:
    download_task = FilesToStorageOperator(
        task_id="download_data",
        files=config.REALTIME_PROVIDERS,
        gcp_conn_id=config.GCP_CONN_ID,
        bucket_name=config.BUCKET_NAME,
    )
    transform_task = TransformStorageFilesOperator(
        task_id="transform_data",
        gcp_conn_id=config.GCP_CONN_ID,
        bucket_name=config.BUCKET_NAME,
        handlers=config.REALTIME_TRANSFORMERS,
    )
    upload_task = StorageFilesToFirestoreOperator(
        task_id="upload_to_firestore",
        gcp_conn_id=config.GCP_CONN_ID,
        bucket_name=config.BUCKET_NAME,
        files=config.REALTIME_MASTER_FILES,
        collection_id="crypto",
        key_column="coin",
    )

    (download_task >> transform_task >> upload_task)
