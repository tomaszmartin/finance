"""Extract historical data for chosen cryptocurrencies."""
import datetime as dt

from airflow import DAG

from app.operators.scraping import FilesToBucketOperator, BucketFilesToFirestoreOperator
from app.dags import crypto


GCP_CONN_ID = "google_cloud"
BUCKET_NAME = "sandbox_data_lake"

crypto_dag = DAG(
    dag_id="crypto_realtime",
    schedule_interval="@daily",
    start_date=dt.datetime.today() - dt.timedelta(days=1),
)

download_task = FilesToBucketOperator(
    task_id="download_coins",
    dag=crypto_dag,
    file_providers=crypto.REALTIME_PROVIDERS,
    gcp_conn_id=GCP_CONN_ID,
    bucket_name=BUCKET_NAME,
)
to_firestore_task = BucketFilesToFirestoreOperator(
    task_id="to_firestore",
    dag=crypto_dag,
    file_parsers=crypto.REALTIME_PARSERS,
    gcp_conn_id=GCP_CONN_ID,
    bucket_name=BUCKET_NAME,
    collection_id="crypto",
    key_column="coin",
)
download_task >> to_firestore_task
