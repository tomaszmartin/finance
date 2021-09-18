"""Contains Operators working on a Google Cloud Storage."""
import json
import logging
from typing import Any, Callable

from airflow.hooks.base_hook import BaseHook
from airflow.models.baseoperator import BaseOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from google.oauth2 import service_account
from google.cloud import firestore

from app.tools.files import to_bytes, from_bytes


class FilesToStorageOperator(BaseOperator):
    """Creates a task that takes artibtrary functions that return bytes and
    saves those bytes in Google Cloud Storage.

    It is helpfull when You want to use Storage as a landing zone for
    any data You migh want to collect, especially when it is not in the
    most convenient form, for example html.
    """

    template_fields = ["files"]

    def __init__(
        self,
        gcp_conn_id: str,
        bucket_name: str,
        files: list[tuple[str, Callable]],
        fail_on_errors: bool = True,
        **kwargs,
    ) -> None:
        """Allows to download a set of files and save them in Google Cloud Storage.

        Args:
            gcp_conn_id: GCP connection for destination bucket.
            bucket_name: Destination bucket name.
            files: List of destination object name, and a callable
                returning bytes data with a following signature:
                    `handler(context) -> bytes`
            fail_on_errors: whether the task should fail on error. Should be used with
                caution since it can silence important errors.
        """
        super().__init__(**kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.bucket_name = bucket_name
        self.files = files
        self.fail_on_errors = fail_on_errors

    def execute(self, context):
        for object_name, file_provider in self.files:
            try:
                data = file_provider(context["execution_date"])
                storage_hook = GCSHook(google_cloud_storage_conn_id=self.gcp_conn_id)
                storage_hook.upload(
                    self.bucket_name, object_name=object_name, data=data
                )
            except Exception as error:
                logging.warning("Failed for %s.", object_name)
                if self.fail_on_errors:
                    raise error


class TransformStorageFilesOperator(BaseOperator):
    """Creates task that takes a list of:
        * source files on Google Cloud Storage
        * destination paths on Google Cloud Storage
        * a transformation function.
    It's helpful when You want to move files from raw format
    to a more structured.
    """

    template_fields = ["handlers"]

    def __init__(
        self,
        gcp_conn_id: str,
        bucket_name: str,
        handlers: list[tuple[str, str, Callable]],
        fail_on_errors: bool = True,
        **kwargs,
    ) -> None:
        """Allows to download a set of files and save them in Google Cloud Storage.
        Args:
            handlers: dict of source object name, destination object name, and a callable
                with following signature
                `handler(original_data, context) -> list[dict]`
                the callable is reponsible for returning data.
            gcp_conn_id: GCP connection for destination bucket
            bucket_name: source and destination bucket name
            fail_on_errors: whether the task should fail on error. Should be used with
                caution since it can silence important errors.
        """
        super().__init__(**kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.bucket_name = bucket_name
        self.handlers = handlers
        self.fail_on_errors = fail_on_errors

    def execute(self, context):
        for src_name, dst_name, handler in self.handlers:
            try:
                storage_hook = GCSHook(google_cloud_storage_conn_id=self.gcp_conn_id)
                original_data = storage_hook.download(self.bucket_name, src_name)
                data = handler(original_data, context["execution_date"])
                byte_data = to_bytes(dst_name, data)
                storage_hook.upload(
                    self.bucket_name, object_name=dst_name, data=byte_data
                )
            except Exception as error:
                logging.warning("Failed for %s.", src_name)
                if self.fail_on_errors:
                    raise error


class StorageFilesToFirestoreOperator(BaseOperator):
    """Creates a task that uploads list of Google Cloud Storage files
    to a certain collection in Google Firestore.
    """

    template_fields = ["files"]

    def __init__(
        self,
        gcp_conn_id: str,
        bucket_name: str,
        files: list[str],
        collection_id,
        key_column,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.bucket_name = bucket_name
        self.files = files
        self.collection_id = collection_id
        self.key_column = key_column

    def _store(self, data: list[dict[str, Any]]) -> None:
        client = self._firestore_client()
        batches = [data[item : item + 500] for item in range(0, len(data), 500)]
        collection = client.collection(self.collection_id)
        for batch in batches:
            firestore_batch = client.batch()
            for row in batch:
                firestore_batch.set(collection.document(row[self.key_column]), row)
            firestore_batch.commit()

    def _firestore_client(self):
        connection = BaseHook.get_connection(self.gcp_conn_id)
        sa_str = connection.extra_dejson["extra__google_cloud_platform__keyfile_dict"]
        sa_json = json.loads(sa_str)
        credentials = service_account.Credentials.from_service_account_info(sa_json)
        client = firestore.Client(credentials=credentials)
        return client

    def execute(self, context):
        for src_name in self.files:
            storage_hook = GCSHook(google_cloud_storage_conn_id=self.gcp_conn_id)
            byte_data = storage_hook.download(self.bucket_name, src_name)
            data = from_bytes(src_name, byte_data)
            self._store(data)
