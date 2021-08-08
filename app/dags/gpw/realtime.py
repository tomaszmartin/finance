"""Extracts current (realtime) data from GPW Polish Stock Exchange."""
import datetime as dt
from functools import partial

from airflow import DAG

from app.scrapers.stocks import prices
from app.operators.scraping import FileToBucketOperator, BucketFileToFirestoreOperator


GCP_CONN_ID = "google_cloud"
BUCKET_NAME = "sandbox_data_lake"
realtime_dag = DAG(
    dag_id="stocks_realtime",
    # Run on workdays each hour from 9 to 17
    schedule_interval="0 9-17 * * 1-5",
    start_date=dt.datetime.today(),
)

for instrument in ["equities", "indices"]:
    OBJ_NAME = "stocks/realtime/{instrument}{{ds}}.html"
    download_task = FileToBucketOperator(
        task_id=f"download_{instrument}",
        dag=realtime_dag,
        file_provider=partial(prices.get_current, instrument),
        gcp_conn_id=GCP_CONN_ID,
        bucket_name=BUCKET_NAME,
        object_name=OBJ_NAME,
    )
    to_firestore_task = BucketFileToFirestoreOperator(
        task_id=f"{instrument}_to_firestore",
        dag=realtime_dag,
        parse_func=prices.parse_realtime,
        gcp_conn_id=GCP_CONN_ID,
        bucket_name=BUCKET_NAME,
        object_name=OBJ_NAME,
        collection_id=instrument,
        key_column="isin_code",
    )
    download_task >> to_firestore_task
