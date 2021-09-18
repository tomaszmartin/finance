"""Contains Operators for working with Google BigQuery."""
from typing import Any
import logging

from airflow.models.baseoperator import BaseOperator
from airflow.contrib.hooks.bigquery_hook import BigQueryHook


class SelectFromBigQuery(BaseOperator):
    """Operator that enables to select data from BigQuery and
    returns it as a list of dicts.
    """

    template_fields = ["sql"]

    def __init__(self, gcp_conn_id: str, sql: str, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.sql = sql

    def execute(self, context: Any) -> list[dict[str, Any]]:
        bq_hook = BigQueryHook(gcp_conn_id=self.gcp_conn_id, use_legacy_sql=False)
        logging.info(self.sql)
        results = bq_hook.get_pandas_df(
            sql=self.sql,
        )
        data = results.to_dict("records")
        return data
