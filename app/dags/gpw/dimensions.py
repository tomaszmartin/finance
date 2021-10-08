"""Extracts information about instruments from GPW Polish Stock Exchange."""
import datetime as dt

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator,
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
)

from app.operators.bigquery import (
    SelectFromBigQueryOperator,
    UpsertGCSToBigQueryOperator,
)
from app.operators.gpw import DimensionToGCSOperator, TransformDimensionOperator


GCP_CONN_ID = "google_cloud"
DATASET_ID = "stocks"
BUCKET_NAME = "stocks_dl"
STOCK_TABLE = "equities"
XCOM_KEY = "get_stocks_list"
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

dimensions_dag = DAG(
    dag_id="gpw_dims",
    description="Scrapes information about equities on GPW.",
    schedule_interval="@daily",
    start_date=dt.datetime.today() - dt.timedelta(days=3),
)

get_isin_codes = SelectFromBigQueryOperator(
    task_id=XCOM_KEY,
    dag=dimensions_dag,
    gcp_conn_id=GCP_CONN_ID,
    sql=f"SELECT DISTINCT(isin_code) FROM `{DATASET_ID}.{STOCK_TABLE}`"
    + ' WHERE date >= "{{ execution_date.subtract(days=3).date() }}"',
)
for fact_type in ["info", "indicators"]:
    TABLE_ID = f"dim_{fact_type}"
    SCHEMA = SCHEMAS[fact_type]
    ARGS = {"process": "gpw", "dataset": f"dim_{fact_type}", "extension": "jsonl"}
    PREFIX = f'master/{ARGS["process"]}/{ARGS["dataset"]}'

    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id=f"create_{fact_type}_dataset",
        dag=dimensions_dag,
        gcp_conn_id=GCP_CONN_ID,
        dataset_id=DATASET_ID,
        location="EU",
    )
    create_table = BigQueryCreateEmptyTableOperator(
        task_id=f"create_{fact_type}_table",
        dag=dimensions_dag,
        bigquery_conn_id=GCP_CONN_ID,
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
        schema_fields=SCHEMA,
        cluster_fields=["isin_code"],
        time_partitioning={"type": "MONTH", "field": "date"},
        exists_ok=True,
    )
    download_raw = DimensionToGCSOperator(
        task_id=f"download_raw_{fact_type}",
        dag=dimensions_dag,
        gcp_conn_id=GCP_CONN_ID,
        bucket_name=BUCKET_NAME,
        fact_type=fact_type,
        from_xcom=XCOM_KEY,
        path_args=ARGS,
    )
    transform_to_master = TransformDimensionOperator(
        task_id=f"transform_{fact_type}_to_master",
        dag=dimensions_dag,
        gcp_conn_id=GCP_CONN_ID,
        bucket_name=BUCKET_NAME,
        fact_type=fact_type,
        from_xcom=XCOM_KEY,
        path_args=ARGS,
    )
    upsert_data = UpsertGCSToBigQueryOperator(
        task_id=f"upsert_to_{TABLE_ID}",
        dag=dimensions_dag,
        gcp_conn_id=GCP_CONN_ID,
        bucket_name=BUCKET_NAME,
        source_prefix=PREFIX,
        source_format="NEWLINE_DELIMITED_JSON",
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
        schema_fields=SCHEMA,
    )

    verify = BigQueryCheckOperator(
        task_id=f"verify_{fact_type}",
        dag=dimensions_dag,
        gcp_conn_id=GCP_CONN_ID,
        sql='SELECT date FROM {{params.table}} WHERE date = "{{ds}}"',
        params={"table": f"{DATASET_ID}.{TABLE_ID}"},
        use_legacy_sql=False,
    )

    (
        get_isin_codes
        >> create_dataset
        >> create_table
        >> download_raw
        >> transform_to_master
        >> upsert_data
        >> verify
    )
