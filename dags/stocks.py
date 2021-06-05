import datetime as dt
from typing import Dict, Any
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator, task
from airflow.operators.dummy import DummyOperator
import requests


def download(
    instrument: str, execution_date: dt.date, **context: Dict[str, Any]
) -> None:
    logger = logging.getLogger("airflow.task")
    logger.info("Downloading %s data for %s", instrument, execution_date)
    type_map = {"equities": "10", "indices": "1"}
    params = {
        "type": type_map[instrument],
        "instrument": "",
        "date": execution_date.strftime("%d-%m-%Y"),
    }
    url = f"https://www.gpw.pl/price-archive-full"
    path = f"data/raw/{instrument}/{execution_date.year}/{execution_date.strftime('%Y-%m-%d')}.html"
    resp = requests.get(url, params=params)
    with open(path, "wb") as file:
        file.write(resp.content)


stocks_dag = DAG(
    dag_id="stocks_dag",
    schedule_interval="@daily",
    start_date=dt.datetime(2021, 6, 1),
)

start_task = DummyOperator(task_id="start", dag=stocks_dag)

download_equities = PythonOperator(
    task_id="download_equities",
    dag=stocks_dag,
    python_callable=download,
    op_kwargs={"instrument": "equities"},
)

download_indices = PythonOperator(
    task_id="download_indices",
    dag=stocks_dag,
    python_callable=download,
    op_kwargs={"instrument": "indices"},
)

start_task >> download_equities
start_task >> download_indices
