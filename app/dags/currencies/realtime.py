"""Extract historical data for currencies."""
import datetime as dt
from functools import partial

from airflow import DAG

from app.operators.scraping import FilesToBucketOperator, BucketFilesToFirestoreOperator
from app.scrapers import currencies


GCP_CONN_ID = "google_cloud"
BUCKET_NAME = "sandbox_data_lake"

OBJ_NAME = "currencies/realtime/currencies.json"
TABLE_ID = "rates"
currencies_dag = DAG(
    dag_id="currencies_realtime",
    schedule_interval="@daily",
    start_date=dt.datetime.today() - dt.timedelta(days=1),
)
download_task = FilesToBucketOperator(
    task_id="download_rates",
    dag=currencies_dag,
    file_providers={OBJ_NAME: partial(currencies.download_data, realtime=True)},
    gcp_conn_id=GCP_CONN_ID,
    bucket_name=BUCKET_NAME,
)
to_firestore_task = BucketFilesToFirestoreOperator(
    task_id="to_firestore",
    dag=currencies_dag,
    file_parsers={OBJ_NAME: currencies.parse_data},
    gcp_conn_id=GCP_CONN_ID,
    bucket_name=BUCKET_NAME,
    collection_id="currencies",
    key_column="currency",
)

download_task >> to_firestore_task
