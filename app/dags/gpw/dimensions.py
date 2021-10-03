"""Extracts information about instruments from GPW Polish Stock Exchange."""
import datetime as dt

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
)

from app.operators.bigquery import SelectFromBigQueryOperator


GCP_CONN_ID = "google_cloud"
DATASET_ID = "stocks"
BUCKET_NAME = "stocks_dl"
STOCK_TABLE = "equities"
CODES_TASK = "get_stocks_list"
SCHEMA = {
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
CLUSTER = ["isin_code"]
PARTITIONING = {"type": "MONTH", "field": "date"}

dimensions_dag = DAG(
    dag_id="gpw_dims",
    schedule_interval="@daily",
    start_date=dt.datetime.today() - dt.timedelta(days=3),
)

get_isin_codes = SelectFromBigQueryOperator(
    task_id=CODES_TASK,
    dag=dimensions_dag,
    gcp_conn_id=GCP_CONN_ID,
    sql=f"SELECT DISTINCT(isin_code) FROM `{DATASET_ID}.{STOCK_TABLE}` "
    + 'WHERE date >= "{{ execution_date.subtract(days=3).date() }}"',
)
for fact_type in ["info", "indicators"]:
    TABLE_ID = f"dim_{fact_type}"
    TEMP_TABLE_ID = TABLE_ID + "_temp{{ ds_nodash }}"

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
        schema_fields=SCHEMA[fact_type],
        cluster_fields=CLUSTER,
        time_partitioning=PARTITIONING,
        exists_ok=True,
    )
    create_temp_table = BigQueryCreateEmptyTableOperator(
        task_id=f"create_temp_{fact_type}_table",
        dag=dimensions_dag,
        bigquery_conn_id=GCP_CONN_ID,
        dataset_id=DATASET_ID,
        table_id=TEMP_TABLE_ID,
        schema_fields=SCHEMA[fact_type],
        cluster_fields=CLUSTER,
        time_partitioning=PARTITIONING,
        exists_ok=True,
    )
    # download raw
    # transform to master
    # upload bq to temp
    # replace in transaction
    # verify data
    # delete temp table
