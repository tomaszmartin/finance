"""Extracts information about instruments from GPW Polish Stock Exchange."""
import datetime as dt
from functools import partial
from typing import Any, Callable, Iterable

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator,
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
)

from app.operators.bigquery import SelectFromBigQuery
from app.operators.scraping import (
    FilesToBucketOperator,
    BucketFilesToBigQueryOperator,
)
from app.scrapers.stocks import dimenions


GCP_CONN_ID = "google_cloud"
DATASET_ID = "stocks"
BUCKET_NAME = "sandbox_data_lake"
STOCK_TABLE = "equities"
CODES_TASK = "get_stocks_list"
SCHEMAS = {
    "info": [
        {"name": "date", "type": "DATE", "mode": "REQUIRED"},
        {"name": "isin_code", "type": "STRING", "mode": "REQUIRED"},
        {"name": "abbreviation", "type": "STRING", "mode": "REQUIRED"},
        {"name": "name", "type": "STRING", "mode": "REQUIRED"},
        {"name": "full_name", "type": "STRING", "mode": "REQUIRED"},
        {"name": "address", "type": "STRING", "mode": "REQUIRED"},
        {"name": "voivodeship", "type": "STRING", "mode": "REQUIRED"},
        {"name": "email", "type": "STRING", "mode": "NULLABLE"},
        {"name": "fax", "type": "STRING", "mode": "NULLABLE"},
        {"name": "phone", "type": "STRING", "mode": "NULLABLE"},
        {"name": "www", "type": "STRING", "mode": "NULLABLE"},
        {"name": "ceo", "type": "STRING", "mode": "REQUIRED"},
        {"name": "regs", "type": "STRING", "mode": "NULLABLE"},
        {"name": "first_listing", "type": "STRING", "mode": "REQUIRED"},
        {"name": "market_value", "type": "FLOAT64", "mode": "REQUIRED"},
        {"name": "number_of_shares_issued", "type": "FLOAT64", "mode": "REQUIRED"},
    ],
    "indicators": [
        {"name": "date", "type": "DATE", "mode": "REQUIRED"},
        {"name": "isin_code", "type": "STRING", "mode": "REQUIRED"},
        {"name": "market", "type": "STRING", "mode": "REQUIRED"},
        {"name": "sector", "type": "STRING", "mode": "REQUIRED"},
        {"name": "book_value", "type": "FLOAT64", "mode": "REQUIRED"},
        {"name": "dividend_yield", "type": "FLOAT64", "mode": "NULLABLE"},
        {"name": "market_value", "type": "FLOAT64", "mode": "REQUIRED"},
        {"name": "number_of_shares_issued", "type": "FLOAT64", "mode": "REQUIRED"},
        {"name": "pbv", "type": "FLOAT64", "mode": "NULLABLE"},
        {"name": "pe", "type": "FLOAT64", "mode": "NULLABLE"},
    ],
}


def get_file_names(
    isin_codes: list[dict[str, str]], fact_type: str, execution_date: dt.date
) -> list[tuple[str, str]]:
    result = []
    for item in isin_codes:
        code = item["isin_code"]
        file = f"stocks/dimensions/{code}/{execution_date}_{fact_type}.html"
        result.append((code, file))
    return result


def get_generator(
    context: Any, xcom_task: str, task: str, fact_type: str
) -> Iterable[tuple[str, Callable]]:
    isin_codes = context["task_instance"].xcom_pull(task_ids=xcom_task)
    file_names = get_file_names(
        isin_codes,
        fact_type,
        context["execution_date"],
    )
    for isin_code, file_path in file_names:
        if task == "download":
            yield file_path, partial(
                dimenions.get_data, isin_code=isin_code, fact_type=fact_type
            )
        elif task == "upload":
            yield file_path, partial(
                dimenions.parse_data, isin_code=isin_code, fact_type=fact_type
            )


facts_dag = DAG(
    dag_id="stocks_dims",
    schedule_interval="@daily",
    start_date=dt.datetime.today() - dt.timedelta(days=1),
)
get_isin_codes = SelectFromBigQuery(
    task_id=CODES_TASK,
    dag=facts_dag,
    gcp_conn_id=GCP_CONN_ID,
    sql=f"SELECT DISTINCT(isin_code) FROM `{DATASET_ID}.{STOCK_TABLE}` "
    + 'WHERE date >= "{{ execution_date.subtract(days=30).date() }}"',
)

for fact_type in ["info", "indicators"]:
    table_id = f"dim_{fact_type}"
    download = FilesToBucketOperator(
        task_id=f"download_{fact_type}_to_storage",
        dag=facts_dag,
        file_providers_generator=partial(
            get_generator,
            xcom_task=CODES_TASK,
            task="download",
            fact_type=fact_type,
        ),
        gcp_conn_id=GCP_CONN_ID,
        bucket_name=BUCKET_NAME,
        fail_on_errors=False,
    )
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id=f"create_{fact_type}_dataset",
        dag=facts_dag,
        bigquery_conn_id=GCP_CONN_ID,
        dataset_id=DATASET_ID,
        location="EU",
    )
    create_table = BigQueryCreateEmptyTableOperator(
        task_id=f"create_{fact_type}_table",
        dag=facts_dag,
        bigquery_conn_id=GCP_CONN_ID,
        dataset_id=DATASET_ID,
        table_id=table_id,
        schema_fields=SCHEMAS[fact_type],
        time_partitioning={
            "type": "MONTH",
            "field": "date",
        },
        exists_ok=True,
    )
    to_bigquery = BucketFilesToBigQueryOperator(
        task_id=f"{fact_type}_to_bigquery",
        dag=facts_dag,
        file_parsers_generator=partial(
            get_generator,
            xcom_task=CODES_TASK,
            task="upload",
            fact_type=fact_type,
        ),
        gcp_conn_id=GCP_CONN_ID,
        bucket_name=BUCKET_NAME,
        dataset_id=DATASET_ID,
        table_id=table_id,
        fail_on_errors=False,
    )
    verify_data = BigQueryCheckOperator(
        task_id=f"verify_{fact_type}_data",
        dag=facts_dag,
        gcp_conn_id=GCP_CONN_ID,
        sql='SELECT date FROM {{params.table}} WHERE date = "{{ds}}"',
        params={"table": f"{DATASET_ID}.{table_id}"},
        use_legacy_sql=False,
    )
    get_isin_codes >> download
    get_isin_codes >> create_dataset
    create_dataset >> create_table
    create_table >> to_bigquery
    download >> to_bigquery
    to_bigquery >> verify_data