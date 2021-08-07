import logging
import datetime as dt
from functools import partial
from typing import Any, Dict

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from airflow.providers.http.hooks.http import HttpHook
import pandas as pd

from app.operators.scraping import FileToBucketOperator, BucketFileToBigQueryOperator


GCP_CONN_ID = "google_cloud"
BUCKET_NAME = "sandbox_data_lake"
DATASET_ID = "crypto"
dag = DAG(
    dag_id="crypto_archive",
    schedule_interval="@daily",
    start_date=dt.datetime.today() - dt.timedelta(days=1),
)
coins = [
    "BTC",
    "ETH",
    "LTC",
    "XRP",
    "ETC",
    "XLM",
    "DASH",
    "BTT",
]


def download_file(coin: str, execution_date: dt.datetime) -> bytes:
    next_day = execution_date + dt.timedelta(days=1)
    hook = HttpHook("GET", http_conn_id="coinapi")
    endpoint = "v1/exchangerate/{coin}/USD/history?period_id=1DAY&time_start={start}&time_end={end}"
    endpoint = endpoint.format(
        coin=coin.upper(), start=execution_date.date(), end=next_day.date()
    )
    resp = hook.run(endpoint)
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
            {"name": "coin", "type": "STRING", "mode": "REQUIRED"},
            {"name": "base", "type": "STRING", "mode": "REQUIRED"},
            {"name": "open", "type": "FLOAT64", "mode": "REQUIRED"},
            {"name": "high", "type": "FLOAT64", "mode": "REQUIRED"},
            {"name": "low", "type": "FLOAT64", "mode": "REQUIRED"},
            {"name": "close", "type": "FLOAT64", "mode": "REQUIRED"},
        ],
        exists_ok=True,
        time_partitioning={
            "type": "MONTH",
            "field": "date",
        },
    )


def parse_file(data: bytes, execution_date: dt.datetime, coin: str = ""):
    if not coin:
        raise ValueError("Need to specify coin!")
    frame = pd.read_json(data)
    frame = frame.rename(
        columns={
            "date": "time_close",
            "rate_open": "open",
            "rate_high": "high",
            "rate_low": "low",
            "rate_close": "close",
        }
    )
    frame = frame.drop(
        columns=["time_period_start", "time_period_end", "time_open", "time_close"]
    )
    frame["date"] = execution_date.date()
    frame["coin"] = coin.upper()
    frame["base"] = "USD"
    return frame.to_dict("records")


for coin in coins:
    coin = coin.lower()
    object_name = f"crypto/archive/{coin}" + "{{ds}}.json"
    download_task = FileToBucketOperator(
        task_id=f"download_{coin}",
        dag=dag,
        file_provider=partial(download_file, coin),
        gcp_conn_id=GCP_CONN_ID,
        bucket_name=BUCKET_NAME,
        object_name=object_name,
    )
    create_table_task = PythonOperator(
        task_id=f"create_{coin}_table",
        dag=dag,
        python_callable=create_table,
        op_kwargs={
            "gcp_conn_id": GCP_CONN_ID,
            "dataset_id": DATASET_ID,
            "table_id": coin,
        },
    )
    to_bigquery_task = BucketFileToBigQueryOperator(
        task_id=f"{coin}_to_bigquery",
        dag=dag,
        parse_func=partial(parse_file, coin=coin),
        gcp_conn_id=GCP_CONN_ID,
        bucket_name=BUCKET_NAME,
        object_name=object_name,
        dataset_id=DATASET_ID,
        table_id=coin,
    )
    verify_data = BigQueryCheckOperator(
        task_id=f"verify_{coin}",
        dag=dag,
        gcp_conn_id=GCP_CONN_ID,
        sql='SELECT date FROM {{params.table}} WHERE date = "{{ds}}"',
        params={"table": f"{DATASET_ID}.{coin}"},
        use_legacy_sql=False,
    )
    download_task >> to_bigquery_task
    create_table_task >> to_bigquery_task
    to_bigquery_task >> verify_data
