"""Verifies the quality of stocks data."""
import datetime as dt

from airflow import DAG
from app.dags.gpw.dimensions import TABLE_ID

from app.operators.bigquery import BigQueryValidateDataOperator


GCP_CONN_ID = "google_cloud"
DATASET_ID = "stocks"

quality_dag = DAG(
    dag_id="gpw_quality",
    description="Verifies the quality of GPW data.",
    schedule_interval="@daily",
    start_date=dt.datetime.today() - dt.timedelta(days=3),
)

for instrument in ["equities", "indices"]:
    TABLE_ID = instrument
    # assert values distinct for key
    check_distinct = BigQueryValidateDataOperator(
        dag=quality_dag,
        task_id=f"check_distinct_{TABLE_ID}",
        gcp_conn_id=GCP_CONN_ID,
        params={"table": f"{DATASET_ID}.{TABLE_ID}"},
        key="cmp",
        sql="""
            SELECT IF(COUNT(isin_code)=COUNT(DISTINCT(isin_code)), TRUE, FALSE) AS cmp
            FROM {{ params.table }} 
            WHERE date >= "{{ execution_date.subtract(days=30).date() }}" GROUP BY date;
            """,
    )
    # assert count +/-10% last day
    check_count = BigQueryValidateDataOperator(
        dag=quality_dag,
        task_id=f"check_count_{TABLE_ID}",
        gcp_conn_id=GCP_CONN_ID,
        params={"table": f"{DATASET_ID}.{TABLE_ID}"},
        key="cmp",
        sql="""
            WITH today AS (
                SELECT COUNT(isin_code) AS cnt
                FROM {{ params.table }} WHERE date = "{{ ds }}" GROUP BY date
            ), yesterday AS (
                SELECT COUNT(isin_code) AS cnt
                FROM {{ params.table }} WHERE date = "{{ prev_ds }}" GROUP BY date
            )
            SELECT
                today.cnt as today,
                yesterday.cnt as yesterday,
                IF((today.cnt/yesterday.cnt > 0.9) AND (today.cnt/yesterday.cnt < 1.1), TRUE, FALSE) as cmp
            FROM today CROSS JOIN yesterday;
            """,
    )
