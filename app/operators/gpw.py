"""Contains Operators for working with GPW dimension data."""
import logging
from typing import Any

from airflow.models.baseoperator import BaseOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from google.api_core.exceptions import NotFound
from requests.exceptions import HTTPError

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
        *,
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

    def execute(self, context: Any) -> None:
        # Get GCS hook
        storage_hook = GCSHook(google_cloud_storage_conn_id=self.gcp_conn_id)
        # Get isin_codes
        rows = context["ti"].xcom_pull(task_ids=self.from_xcom)
        codes = [row["isin_code"] for row in rows]
        errors = 0
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
            except HTTPError:
                logging.warning("Error downloading data for %s.", code)
                errors += 1
        _check_error_rate(errors, len(codes))


class TransformDimensionOperator(BaseOperator):
    """Transforms dimensions data stored on GCS from raw
    'html' format into json newline files."""

    def __init__(
        self,
        *,
        gcp_conn_id: str,
        bucket_name: str,
        fact_type: str,
        from_xcom: str,
        raw_args: dict[str, str],
        master_args: dict[str, str],
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.bucket_name = bucket_name
        self.fact_type = fact_type
        self.from_xcom = from_xcom
        self.raw_args = raw_args
        self.master_args = master_args

    def execute(self, context: Any) -> None:
        # Get GCS hook
        storage_hook = GCSHook(google_cloud_storage_conn_id=self.gcp_conn_id)
        # Get isin_codes
        rows = context["ti"].xcom_pull(task_ids=self.from_xcom)
        codes = [row["isin_code"] for row in rows]
        errors = 0
        for code in codes:
            try:
                # Download data
                src_name = datalake.raw(**self.raw_args, prefix=code)
                src_name = self.render_template(src_name, context)
                original_data = storage_hook.download(self.bucket_name, src_name)
                # Transform
                data = dimensions.parse_data(
                    original_data,
                    isin_code=code,
                    fact_type=self.fact_type,
                    execution_date=context["data_interval_start"],
                )
                if not data:
                    logging.warning("Empty data for %s.", code)
                    continue
                # Upload to GCS
                dst_name = datalake.master(**self.master_args, prefix=code)
                dst_name = self.render_template(dst_name, context)
                byte_data = to_bytes(dst_name, data)
                storage_hook.upload(
                    self.bucket_name, object_name=dst_name, data=byte_data
                )
            except NotFound:
                logging.warning("Error downloading data for %s.", code)
                errors += 1
        _check_error_rate(errors, len(codes))


def _check_error_rate(errors: int, total: int) -> None:
    if (errors / total) < 0.05:
        return
    raise ValueError("Too many errors for dimensions.")
