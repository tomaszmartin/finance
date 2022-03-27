"""Contains Operators working on a Google Cloud Storage."""
import datetime as dt
from typing import Any

from airflow.models.baseoperator import BaseOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from typing_extensions import Protocol

from app.tools.files import to_bytes


# pylint: disable=too-few-public-methods
class ObjectProvider(Protocol):
    """Type for object provider functions."""

    def __call__(self, execution_date: dt.datetime) -> bytes:
        pass


# pylint: disable=too-few-public-methods
class Transformation(Protocol):
    """Type for transformation functions."""

    def __call__(self, raw_data: bytes, execution_date: dt.datetime) -> list[dict]:
        pass


class FileToGCSOperator(BaseOperator):
    """Creates a task that takes arbitrary functions that return bytes and
    saves those bytes in Google Cloud Storage.

    It is helpfull when You want to use Storage as a landing zone for
    any data You might want to collect, especially when it is not in the
    most convenient form, for example html.
    """

    template_fields = ["object_name"]

    def __init__(
        self,
        gcp_conn_id: str,
        bucket_name: str,
        object_name: str,
        object_provider: ObjectProvider,
        **kwargs: Any,
    ) -> None:
        """Allows to download a set of files and save them in Google Cloud Storage.

        Args:
            gcp_conn_id: GCP connection for destination bucket.
            bucket_name: Destination bucket name.
        """
        super().__init__(**kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.bucket_name = bucket_name
        self.object_name = object_name
        self.object_provider = object_provider

    def execute(self, context: Any) -> None:
        data = self.object_provider(context["execution_date"])
        storage_hook = GCSHook(google_cloud_storage_conn_id=self.gcp_conn_id)
        storage_hook.upload(self.bucket_name, object_name=self.object_name, data=data)


class TransformGCSFileOperator(BaseOperator):
    """Creates task that takes a list of:
        * source files on Google Cloud Storage
        * destination paths on Google Cloud Storage
        * a transformation function.
    It's helpful when You want to move files from raw format
    to a more structured.
    """

    template_fields = ["source_object", "destination_object"]

    def __init__(
        self,
        *,
        gcp_conn_id: str,
        bucket_name: str,
        source_object: str,
        destination_object: str,
        transformation: Transformation,
        **kwargs: Any,
    ) -> None:
        """Allows to download a set of files and save them in Google Cloud Storage.
        Args:
            gcp_conn_id: GCP connection for destination bucket
            bucket_name: source and destination bucket name
        """
        super().__init__(**kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.bucket_name = bucket_name
        self.source_object = source_object
        self.destination_object = destination_object
        self.transformation = transformation

    def execute(self, context: Any) -> None:
        storage_hook = GCSHook(google_cloud_storage_conn_id=self.gcp_conn_id)
        original_data = storage_hook.download(self.bucket_name, self.source_object)
        data = self.transformation(original_data, context["execution_date"])
        byte_data = to_bytes(self.destination_object, data)
        storage_hook.upload(
            self.bucket_name, object_name=self.destination_object, data=byte_data
        )
