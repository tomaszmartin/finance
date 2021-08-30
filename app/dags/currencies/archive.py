"""Extract historical data for currencies."""
import datetime as dt

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator,
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
)

from app.operators.scraping import FilesToBucketOperator, BucketFilesToBigQueryOperator
from app.scrapers import currencies


GCP_CONN_ID = "google_cloud"
BUCKET_NAME = "sandbox_data_lake"
DATASET_ID = "currencies"

OBJ_NAME = "currencies/archive/{{ds}}.json"
TABLE_ID = "rates"
currencies_dag = DAG(
    dag_id="currencies_archive",
    schedule_interval="@daily",
    start_date=dt.datetime.today() - dt.timedelta(days=1),
)
download_task = FilesToBucketOperator(
    task_id="download_rates",
    dag=currencies_dag,
    file_providers={OBJ_NAME: currencies.download_data},
    gcp_conn_id=GCP_CONN_ID,
    bucket_name=BUCKET_NAME,
)
create_dataset = BigQueryCreateEmptyDatasetOperator(
    task_id="create_dataset",
    dag=currencies_dag,
    bigquery_conn_id=GCP_CONN_ID,
    dataset_id=DATASET_ID,
    location="EU",
)
create_table = BigQueryCreateEmptyTableOperator(
    task_id="create_table",
    dag=currencies_dag,
    bigquery_conn_id=GCP_CONN_ID,
    dataset_id=DATASET_ID,
    table_id=TABLE_ID,
    schema_fields=[
        {"name": "date", "type": "DATE", "mode": "REQUIRED"},
        {"name": "currency", "type": "STRING", "mode": "REQUIRED"},
        {"name": "base", "type": "STRING", "mode": "REQUIRED"},
        {"name": "rate", "type": "FLOAT64", "mode": "REQUIRED"},
    ],
    time_partitioning={
        "type": "MONTH",
        "field": "date",
    },
    exists_ok=True,
)
to_bigquery_task = BucketFilesToBigQueryOperator(
    task_id="rates_to_bigquery",
    dag=currencies_dag,
    file_parsers={OBJ_NAME: currencies.parse_data},
    gcp_conn_id=GCP_CONN_ID,
    bucket_name=BUCKET_NAME,
    dataset_id=DATASET_ID,
    table_id=TABLE_ID,
)
verify_data = BigQueryCheckOperator(
    task_id="verify_rates",
    dag=currencies_dag,
    gcp_conn_id=GCP_CONN_ID,
    sql='SELECT date FROM {{params.table}} WHERE date = "{{ds}}"',
    params={"table": f"{DATASET_ID}.{TABLE_ID}"},
    use_legacy_sql=False,
)

download_task >> to_bigquery_task
create_dataset >> create_table
create_table >> to_bigquery_task
to_bigquery_task >> verify_data
