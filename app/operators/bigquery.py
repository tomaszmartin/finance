"""Contains Operators for working with Google BigQuery."""
import logging
import uuid
from typing import Any, Dict, Optional

import pandas as pd
from airflow.models.baseoperator import BaseOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook

from app.tools import sql as sql_generators


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
        results = bq_hook.get_pandas_df(sql=self.sql)
        data = results.to_dict("records")
        # to_dict can return list or single dict/mapping
        if not isinstance(data, list):
            return [data]
        return data


# pylint: disable=too-many-instance-attributes
class UpsertGCSToBigQueryOperator(BaseOperator):
    """Upsert data from Google Cloud Storage objects
    into Google BigQuery table.

    Upsert is divided into following phases:
        * Creating external table from GCS objects.
        * Extracting values from external table for 'delete_using' key.
        * Deleting rows from destination table with extracted values.
        * Appending external table to destination table.
        * Dropping external table.
    """

    template_fields = ["source_objects"]

    def __init__(
        self,
        *,
        gcp_conn_id: str,
        bucket_name: str,
        source_format: str,
        dataset_id: str,
        table_id: str,
        schema_fields: list[dict[str, Any]],
        temp_table_id: Optional[str] = None,
        source_objects: Optional[list[str]] = None,
        source_prefix: Optional[str] = None,
        overwrite_using: str = "date",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.bucket_name = bucket_name
        self.source_objects = source_objects
        self.source_prefix = source_prefix
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.schema_fields = schema_fields
        self.source_format = source_format
        self.temp_table_id = temp_table_id
        self.overwrite_using = overwrite_using

    def execute(self, context: Any) -> None:
        bq_hook = BigQueryHook(gcp_conn_id=self.gcp_conn_id, use_legacy_sql=False)
        temp_table_id = self.temp_table_id or f"{self.table_id}_{uuid.uuid4()}"
        try:
            self.create_external_table(context, bq_hook, temp_table_id)
            self.overwrite_data(
                bq_hook, src_table=temp_table_id, dest_table=self.table_id
            )
        finally:
            self.drop_external_table(bq_hook, temp_table_id)

    def overwrite_data(
        self, bq_hook: BigQueryHook, src_table: str, dest_table: str
    ) -> None:
        """Overwrites data in 'dest_table' with data
        in 'src_table' using 'overwrite_using' specified
        in class constructor.

        Args:
            bq_hook: BigQuery hook for provided 'gcp_conn_id'.
            src_table: Source table ID.
            dest_table: Destination table ID.
        """
        replace_query = sql_generators.overwrite_in_transaction(
            dataset_id=self.dataset_id,
            src_table=src_table,
            dest_table=dest_table,
            overwrite_using=self.overwrite_using,
        )
        bq_hook.insert_job(configuration=self._query_config(replace_query))

    def create_external_table(
        self,
        context: Any,
        bq_hook: BigQueryHook,
        table_id: str,
    ) -> None:
        """Creates external table from specified Cloud Storage
        objects.

        Args:
            context: Airflow context.
            bq_hook: BigQuery hook for provided 'gcp_conn_id'.
            table_id: BigQuery table ID.

        Raises:
            TableExistsError: If table already exists.
        """
        exists = bq_hook.table_exists(dataset_id=self.dataset_id, table_id=table_id)
        if exists:
            msg = f"Can't create temp table: `{table_id}` already exists."
            raise TableExistsError(msg)
        source_uris = self.get_source_uris(filter_using=context["ds_nodash"])
        bq_hook.create_external_table(
            external_project_dataset_table=f"{self.dataset_id}.{table_id}",
            source_format=self.source_format,
            schema_fields=self.schema_fields,
            source_uris=source_uris,
        )

    def drop_external_table(self, bq_hook: BigQueryHook, table_id: str) -> None:
        """Drops the specified table.

        Args:
            bq_hook: BigQuery hook that executes queries.
            table_id: ID of the table that should be dropped.
        """
        drop_query = f"DROP TABLE `{self.dataset_id}.{table_id}`"
        bq_hook.insert_job(configuration=self._query_config(drop_query))

    def get_source_uris(self, filter_using: str) -> list[str]:
        """Returns source URIs. URIs can be created:
            * Using provided source objects and bucket name.
            * Using provided source prefix by listing files in the bucket
              and picking files with the same prefix.
        Both methods can be used to return the full list.

        Args:
            filter_using: Allows to filter results.

        Returns:
            List of source URIs found.

        Raises:
            ValueError: When no URIs are found instead of returning
             empty list the method raises ValueError.
        """
        source_uris = []
        if self.source_objects:
            source_uris.extend(
                [f"gs://{self.bucket_name}/{src}" for src in self.source_objects]
            )
        if self.source_prefix:
            storage_hook = GCSHook(google_cloud_storage_conn_id=self.gcp_conn_id)
            result = storage_hook.list(self.bucket_name, prefix=self.source_prefix)
            result = [path for path in result if filter_using in path]
            result = [f"gs://{self.bucket_name}/{src}" for src in result]
            source_uris.extend(result)

        logging.info("Found %s source URIs.", len(source_uris))
        if not source_uris:
            raise ValueError("No source URIs found.")
        return source_uris

    @staticmethod
    def _query_config(query: str) -> dict[str, Any]:
        return {"query": {"query": query, "useLegacySql": False}}


class BigQueryValidateDataOperator(BaseOperator):
    """Operator that verifies whether data returned from given query
    is true for all rows on certain column.

    For example when given this query:

        'SELECT date, IF(SUM(xyz) > 0, TRUE, FALSE) AS cmp GROUP BY date;'

        That returns following data:

        date       | cmp
        2021-01-02 | true
        2021-01-01 | false

        It checks whether all values in key (here 'cmp') are true.
        If not it raises AssertionError.
    """

    template_fields = ["query"]

    def __init__(
        self,
        *,
        gcp_conn_id: str,
        query: str,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.query = query

    def execute(self, context: Any) -> None:
        results = self.get_data_from_bq(self.query)
        data = results.to_dict("records")
        for row in data:
            if not self.verify(row):
                raise AssertionError(f"Condition not met for {row}.")

    def get_data_from_bq(self, query: str) -> pd.DataFrame:
        """Extracts data from BigQuery using provided
        'gcp_conn_id' and query from 'query'.

        Args:
            query: BigQuery in Standard SQL format.

        Returns:
            DataFrame with data returned from sql query.
        """
        bq_hook = BigQueryHook(gcp_conn_id=self.gcp_conn_id, use_legacy_sql=False)
        results = bq_hook.get_pandas_df(query)
        return results

    @staticmethod
    def verify(row: Dict[str, Any]) -> bool:
        """Check whether conditions for a row are met meaning
        all values should be True and row should not be empty.

        Args:
            row: Dictionary with data.

        Returns:
            True if all conditions are met.
        """
        empty = not row
        all_true = all(row.values())
        return not empty and all_true


class TableExistsError(Exception):
    """Raised when trying to create a table that already exists."""
