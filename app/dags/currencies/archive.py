import datetime as dt
import json
import logging
from typing import Any, Dict

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from airflow.providers.http.hooks.http import HttpHook
import requests

from app.operators.scraping import FileToBucketOperator, BucketFileToBigQueryOperator


GCP_CONN_ID = "google_cloud"
BUCKET_NAME = "sandbox_data_lake"
DATASET_ID = "currencies"
dag = DAG(
    dag_id="currencies_archive",
    schedule_interval="@daily",
    start_date=dt.datetime.today() - dt.timedelta(days=1),
)


def download_file(execution_date: dt.datetime) -> bytes:
    endpoint = "https://api.exchangerate.host/{day}?base=PLN"
    endpoint = endpoint.format(day=execution_date.date())
    resp = requests.get(endpoint)
    data = resp.content
    return data


def create_table(
    dataset_id: str,
    table_id: str,
    gcp_conn_id: str,
    **context: Dict[str, Any],
) -> None:
    bq_hook = BigQueryHook(gcp_conn_id=gcp_conn_id)
    bq_hook.create_empty_dataset(
        dataset_id=dataset_id,
        exists_ok=True,
        location="EU",
    )
    bq_hook.create_empty_table(
        dataset_id=dataset_id,
        table_id=table_id,
        schema_fields=[
            {"name": "date", "type": "DATE", "mode": "REQUIRED"},
            {"name": "currency", "type": "STRING", "mode": "REQUIRED"},
            {"name": "base", "type": "STRING", "mode": "REQUIRED"},
            {"name": "rate", "type": "FLOAT64", "mode": "REQUIRED"},
        ],
        exists_ok=True,
        time_partitioning={
            "type": "MONTH",
            "field": "date",
        },
    )


def parse_file(data: bytes, execution_date: dt.datetime):
    json_str = data.decode("utf-8")
    logging.info(json_str)
    parsed = json.loads(json_str)
    results = []
    for currency, rate in parsed["rates"].items():
        results.append(
            {
                "date": parsed["date"],
                "base": parsed["base"],
                "currency": currency,
                "rate": rate,
            }
        )
    return results


object_name = "currencies/archive/{{ds}}.json"
table_id = "rates"
download_task = FileToBucketOperator(
    task_id=f"download_rates",
    dag=dag,
    file_provider=download_file,
    gcp_conn_id=GCP_CONN_ID,
    bucket_name=BUCKET_NAME,
    object_name=object_name,
)
create_table_task = PythonOperator(
    task_id="create_rates_table",
    dag=dag,
    python_callable=create_table,
    op_kwargs={
        "gcp_conn_id": GCP_CONN_ID,
        "dataset_id": DATASET_ID,
        "table_id": table_id,
    },
)
to_bigquery_task = BucketFileToBigQueryOperator(
    task_id="rates_to_bigquery",
    dag=dag,
    parse_func=parse_file,
    gcp_conn_id=GCP_CONN_ID,
    bucket_name=BUCKET_NAME,
    object_name=object_name,
    dataset_id=DATASET_ID,
    table_id=table_id,
)
verify_data = BigQueryCheckOperator(
    task_id=f"verify_rates",
    dag=dag,
    gcp_conn_id=GCP_CONN_ID,
    sql='SELECT date FROM {{params.table}} WHERE date = "{{ds}}"',
    params={"table": f"{DATASET_ID}.{table_id}"},
    use_legacy_sql=False,
)

download_task >> to_bigquery_task
create_table_task >> to_bigquery_task
to_bigquery_task >> verify_data
