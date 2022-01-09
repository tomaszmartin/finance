"""Verifies the quality of stocks data."""
import datetime as dt

from airflow import DAG

from app.operators.bigquery import BigQueryValidateDataOperator
from app.dags.gpw import config
from app.tools import sql


with DAG(
    dag_id="gpw_quality",
    description="Verifies the quality of GPW data.",
    schedule_interval="@daily",
    start_date=dt.datetime(2021, 11, 1),
    catchup=False,
) as dag:
    for instrument in ["equities", "indices"]:
        TABLE = config.TABLES[instrument]
        TABLE_ID = f"{config.DATASET_ID}.{TABLE}"

        # Assert values distinct for key
        check_distinct = BigQueryValidateDataOperator(
            task_id=f"check_distinct_{TABLE_ID}",
            gcp_conn_id=config.GCP_CONN_ID,
            sql=sql.distinct(
                column="isin_code",
                table_id=TABLE_ID,
                where="date >= '{{ execution_date.subtract(days=30).date() }}'",
                groupby="date",
            ),
        )
        # Assert count +/-10% last day
        check_count = BigQueryValidateDataOperator(
            task_id=f"check_count_{TABLE_ID}",
            gcp_conn_id=config.GCP_CONN_ID,
            sql=sql.count_in_time("isin_code", TABLE_ID),
        )
