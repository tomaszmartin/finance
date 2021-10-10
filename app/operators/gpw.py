"""Contains Operators for working with GPW dimension data."""
import logging
from typing import Any

from airflow.models.baseoperator import BaseOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook

from app.scrapers.stocks import dimensions
from app.tools import datalake
from app.tools.files import to_bytes


class DimensionToGCSOperator(BaseOperator):
    """Downloads dimensions about companies rates
    on GPW. It extracts list of isin codes by
    pulling from 'from_xcom' argument.
    """

    def __init__(
        self,
        gcp_conn_id: str,
        bucket_name: str,
        fact_type: str,
        from_xcom: str,
        path_args: dict[str, str],
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.bucket_name = bucket_name
        self.fact_type = fact_type
        self.from_xcom = from_xcom
        self.path_args = path_args

    def execute(self, context: Any):
        # Get GCS hook
        storage_hook = GCSHook(google_cloud_storage_conn_id=self.gcp_conn_id)
        # Get isin_codes
        rows = context["ti"].xcom_pull(task_ids=self.from_xcom)
        codes = [row["isin_code"] for row in rows]
        # For code
        for code in codes:
            try:
                # Download data
                data = dimensions.get_data(isin_code=code, fact_type=self.fact_type)
                # Upload to GCS
                object_name = datalake.raw(**self.path_args, prefix=code)
                object_name = self.render_template(object_name, context)
                storage_hook.upload(
                    bucket_name=self.bucket_name, object_name=object_name, data=data
                )
            except Exception:
                logging.exception("Error with %s.", code)


class TransformDimensionOperator(BaseOperator):
    """Transforms dimenions data stored on GCS from raw
    'html' format into json newline files."""

    def __init__(
        self,
        gcp_conn_id: str,
        bucket_name: str,
        fact_type: str,
        from_xcom: str,
        path_args: dict[str, str],
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.bucket_name = bucket_name
        self.fact_type = fact_type
        self.from_xcom = from_xcom
        self.path_args = path_args

    def execute(self, context: Any):
        # Get GCS hook
        storage_hook = GCSHook(google_cloud_storage_conn_id=self.gcp_conn_id)
        # Get isin_codes
        rows = context["ti"].xcom_pull(task_ids=self.from_xcom)
        codes = [row["isin_code"] for row in rows]
        for code in codes:
            try:
                # Download data
                src_name = datalake.raw(**self.path_args, prefix=code)
                src_name = self.render_template(src_name, context)
                original_data = storage_hook.download(self.bucket_name, src_name)
                # Transform
                data = dimensions.parse_data(
                    original_data,
                    isin_code=code,
                    fact_type=self.fact_type,
                    execution_date=context["execution_date"],
                )
                # Upload to GCS
                dst_name = datalake.master(**self.path_args, prefix=code)
                dst_name = self.render_template(dst_name, context)
                byte_data = to_bytes(dst_name, data)
                storage_hook.upload(
                    self.bucket_name, object_name=dst_name, data=byte_data
                )
            except Exception:
                logging.exception("Error with %s.", code)
