"""Verifies the quality of stocks data."""
import datetime as dt

from airflow import DAG


GCP_CONN_ID = "google_cloud"
DATASET_ID = "stocks"

dag = DAG(
    dag_id="stocks_quality",
    schedule_interval=None,
    start_date=dt.datetime(2021, 8, 1),
)

# assert values distinct
"""
SELECT IF(COUNT(isin_code)=COUNT(DISTINCT(isin_code)), TRUE, FALSE) AS cmp
FROM {{ params.table }} WHERE date = "2021-09-04" GROUP BY date;
"""

# assert count +/-10% last day
"""
WITH today AS (
    SELECT COUNT(isin_code) AS cnt
    FROM {{ params.table }} WHERE date = "{{ ds }}" GROUP BY date
), yesterday AS (
    SELECT COUNT(isin_code) AS cnt
    FROM {{ params.table }} WHERE date = "{{ prev_ds }}" GROUP BY date
)
SELECT
    today.cnt,
    yesterday.cnt,
    IF((today.cnt/yesterday.cnt > 0.9) AND (today.cnt/yesterday.cnt < 1.1), TRUE, FALSE)
FROM today CROSS JOIN yesterday;
"""

# assert isin_codes match in all tables
"""
WITH eqt AS (
    SELECT date, COUNT(isin_code) AS facts_count
    FROM {{ params.equities }} WHERE date = "{{ ds }}" GROUP BY date
), inf AS (
    SELECT date, COUNT(isin_code) AS info_count
    FROM {{ params.info }} WHERE date = "{{ ds }}" GROUP BY date
), ind AS (
    SELECT date, COUNT(isin_code) AS ind_count
    FROM {{ params.indicators }} WHERE date = "{{ ds }}" GROUP BY date
), tmp AS (
    SELECT eqt.date, eqt.facts_count, inf.info_count
    FROM eqt LEFT JOIN inf ON eqt.date=inf.date
), final AS (
    SELECT tmp.date, tmp.facts_count, tmp.info_count, ind.ind_count
    FROM tmp LEFT JOIN ind ON tmp.date=ind.date
)
SELECT IF(facts_count = info_count AND facts_count = ind_count, TRUE, FALSE) from final;
"""
