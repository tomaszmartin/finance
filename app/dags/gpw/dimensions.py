"""Extracts information about instruments from GPW Polish Stock Exchange."""
import datetime as dt

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator,
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
)

from app.dags.gpw import config
from app.operators.bigquery import (
    SelectFromBigQueryOperator,
    UpsertGCSToBigQueryOperator,
)
from app.operators.gpw import DimensionToGCSOperator, TransformDimensionOperator

with DAG(
    dag_id="gpw_dims",
    description="Scrapes information about equities on GPW.",
    schedule_interval="@monthly",
    start_date=dt.datetime(2021, 11, 1),
    catchup=False,
) as dag:
    XCOM_KEY = "get_stocks_list"
    get_isin_codes = SelectFromBigQueryOperator(
        task_id=XCOM_KEY,
        gcp_conn_id=config.GCP_CONN_ID,
        sql="SELECT DISTINCT(isin_code) FROM "
        + f"`{config.DATASET_ID}.{config.HISTORY_EQUITIES_TABLE_ID}`"
        + " WHERE date >= '{{ execution_date.subtract(days=31).date() }}'",
    )
    for fact_type in ["info", "indicators", "finance"]:
        TABLE_ID = config.TABLES[fact_type]
        SCHEMA = config.SCHEMAS[fact_type]
        RAW_ARGS = {
            "process": "gpw",
            "dataset": f"dim_{fact_type}",
            "extension": "html",
        }
        MASTER_ARGS = {**RAW_ARGS, **{"extension": "jsonl"}}
        PREFIX = f'master/{RAW_ARGS["process"]}/{RAW_ARGS["dataset"]}'

        create_dataset = BigQueryCreateEmptyDatasetOperator(
            task_id=f"create_{fact_type}_dataset",
            gcp_conn_id=config.GCP_CONN_ID,
            dataset_id=config.DATASET_ID,
            location="EU",
        )
        create_table = BigQueryCreateEmptyTableOperator(
            task_id=f"create_{fact_type}_table",
            bigquery_conn_id=config.GCP_CONN_ID,
            dataset_id=config.DATASET_ID,
            table_id=TABLE_ID,
            schema_fields=SCHEMA,
            cluster_fields=["isin_code"],
            time_partitioning={"type": "MONTH", "field": "date"},
            exists_ok=True,
        )
        download_raw = DimensionToGCSOperator(
            task_id=f"download_raw_{fact_type}",
            gcp_conn_id=config.GCP_CONN_ID,
            bucket_name=config.BUCKET_NAME,
            fact_type=fact_type,
            from_xcom=XCOM_KEY,
            path_args=RAW_ARGS,
            execution_timeout=dt.timedelta(minutes=30),
        )
        transform_to_master = TransformDimensionOperator(
            task_id=f"transform_{fact_type}_to_master",
            gcp_conn_id=config.GCP_CONN_ID,
            bucket_name=config.BUCKET_NAME,
            fact_type=fact_type,
            from_xcom=XCOM_KEY,
            raw_args=RAW_ARGS,
            master_args=MASTER_ARGS,
            execution_timeout=dt.timedelta(minutes=30),
        )
        upsert_data = UpsertGCSToBigQueryOperator(
            task_id=f"upsert_to_{TABLE_ID}",
            gcp_conn_id=config.GCP_CONN_ID,
            bucket_name=config.BUCKET_NAME,
            source_prefix=PREFIX,
            source_format="NEWLINE_DELIMITED_JSON",
            dataset_id=config.DATASET_ID,
            table_id=TABLE_ID,
            schema_fields=SCHEMA,
        )
        verify = BigQueryCheckOperator(
            task_id=f"verify_{fact_type}",
            gcp_conn_id=config.GCP_CONN_ID,
            sql="SELECT date FROM {{ params.table }} WHERE date = '{{ ds }}'",
            params={"table": f"{config.DATASET_ID}.{TABLE_ID}"},
            use_legacy_sql=False,
        )

        # pylint: disable=pointless-statement
        (create_dataset >> create_table >> upsert_data)
        (get_isin_codes >> download_raw >> transform_to_master >> upsert_data >> verify)
