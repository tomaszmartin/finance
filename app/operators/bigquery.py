"""Contains Operators for working with Google BigQuery."""
from typing import Any, Optional
import logging

from airflow.models.baseoperator import BaseOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

from app.tools.sql import replace_from_temp


class SelectFromBigQueryOperator(BaseOperator):
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


class UpsertGCSToBigQueryOperator(BaseOperator):

    template_fields = ["source_objects"]

    def __init__(
        self,
        gcp_conn_id: str,
        bucket: str,
        source_objects: list[str],
        source_format: str,
        dataset_id: str,
        table_id: str,
        schema_fields: list[dict[str, Any]],
        temp_table_id: Optional[str] = None,
        delete_using: str = "date",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.bucket = bucket
        self.source_objects = source_objects
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.schema_fields = schema_fields
        self.source_format = source_format
        self.temp_table_id = temp_table_id
        self.delete_using = delete_using

    def execute(self, context: Any):
        bq_hook = BigQueryHook(gcp_conn_id=self.gcp_conn_id, use_legacy_sql=False)
        temp_table_id = self.temp_table_id
        table_id = self.table_id
        if not temp_table_id:
            temp_table_id = f"{table_id}_tmp"
        # Check if temp table exists
        exists = bq_hook.table_exists(
            dataset_id=self.dataset_id, table_id=temp_table_id
        )
        if exists:
            msg = f"Can't create temp table: {temp_table_id} already exists."
            raise ValueError(msg)
        # Create external temp table
        source_uris = [f"gs://{self.bucket}/{src}" for src in self.source_objects]
        bq_hook.create_external_table(
            external_project_dataset_table=f"{self.dataset_id}.{temp_table_id}",
            source_format=self.source_format,
            schema_fields=self.schema_fields,
            source_uris=source_uris,
        )
        replace_query = replace_from_temp(
            dataset_id=self.dataset_id,
            dest_table=self.table_id,
            temp_table=temp_table_id,
            delete_using=self.delete_using,
        )
        bq_hook.insert_job(
            configuration={"query": {"query": replace_query, "useLegacySql": False}}
        )
        bq_hook.insert_job(
            configuration={
                "query": {
                    "query": f"DROP TABLE {self.dataset_id}.{temp_table_id}",
                    "useLegacySql": False,
                }
            }
        )
