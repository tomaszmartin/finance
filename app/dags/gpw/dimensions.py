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
from app.dags.gpw import config


with DAG(
    dag_id="gpw_dims",
    description="Scrapes information about equities on GPW.",
    schedule_interval="@daily",
    start_date=dt.datetime.today() - dt.timedelta(days=3),
) as dag:
    XCOM_KEY = "get_stocks_list"

    get_isin_codes = SelectFromBigQueryOperator(
        task_id=XCOM_KEY,
        gcp_conn_id=config.GCP_CONN_ID,
        sql="SELECT DISTINCT(isin_code) FROM "
        + f"`{config.DATASET_ID}.{config.HISTORY_EQUITIES_TABLE_ID}`"
        + " WHERE date >= '{{ execution_date.subtract(days=3).date() }}'",
    )
    for fact_type in ["info", "indicators"]:
        TABLE_ID = config.TABLES[fact_type]
        SCHEMA = config.SCHEMAS[fact_type]
        ARGS = {"process": "gpw", "dataset": f"dim_{fact_type}", "extension": "jsonl"}
        PREFIX = f'master/{ARGS["process"]}/{ARGS["dataset"]}'

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
            path_args=ARGS,
        )
        transform_to_master = TransformDimensionOperator(
            task_id=f"transform_{fact_type}_to_master",
            gcp_conn_id=config.GCP_CONN_ID,
            bucket_name=config.BUCKET_NAME,
            fact_type=fact_type,
            from_xcom=XCOM_KEY,
            path_args=ARGS,
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
        (
            get_isin_codes
            >> create_dataset
            >> create_table
            >> download_raw
            >> transform_to_master
            >> upsert_data
            >> verify
        )
