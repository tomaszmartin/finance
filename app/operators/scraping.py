import json
import logging
from typing import Any, Callable, Optional

from airflow.hooks.base_hook import BaseHook
from airflow.models.baseoperator import BaseOperator
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from google.oauth2 import service_account
from google.cloud import firestore


class FilesToBucketOperator(BaseOperator):
    template_fields = ["file_providers"]

    def __init__(
        self,
        gcp_conn_id: str,
        bucket_name: str,
        file_providers: Optional[dict[str, Callable]] = None,
        file_providers_generator: Optional[Callable] = None,
        fail_on_errors: bool = True,
        **kwargs,
    ) -> None:
        """Allows to download a set of files and save them in Google Cloud Storage.

        Args:
            file_providers: dict of destination object name, and a callable
                containing function that takes execution_date and returns the file as BytesIO
            gcp_conn_id: GCP connection for destination bucket
            bucket_name: destination bucket name
        """
        super().__init__(**kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.bucket_name = bucket_name
        self.file_providers = file_providers
        self.file_providers_generator = file_providers_generator
        self.fail_on_errors = fail_on_errors

    def execute(self, context):
        files = get_files(self.file_providers, self.file_providers_generator, context)
        for object_name, file_provider in files:
            try:
                data = file_provider(context["execution_date"])
                storage_hook = GCSHook(google_cloud_storage_conn_id=self.gcp_conn_id)
                storage_hook.upload(
                    self.bucket_name, object_name=object_name, data=data
                )
            except Exception as error:
                logging.warning("Failed for %s.", object_name, error)
                if self.fail_on_errors:
                    raise error


class BucketFilesToDatabase(BaseOperator):
    template_fields = ["file_parsers"]

    def __init__(
        self,
        gcp_conn_id: str,
        bucket_name: str,
        file_parsers: Optional[dict[str, Callable]] = None,
        file_parsers_generator: Optional[Callable] = None,
        fail_on_errors: bool = True,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.file_parsers = file_parsers
        self.gcp_conn_id = gcp_conn_id
        self.bucket_name = bucket_name
        self.file_parsers_generator = file_parsers_generator
        self.fail_on_errors = fail_on_errors

    def execute(self, context):
        logging.info("Parse and upload to BigQuery for %s", context["execution_date"])
        parsed_data = []
        files = get_files(self.file_parsers, self.file_parsers_generator, context)
        for object_name, parse_func in files:
            try:
                storage_hook = GCSHook(google_cloud_storage_conn_id=self.gcp_conn_id)
                raw_data = storage_hook.download(
                    object_name=object_name, bucket_name=self.bucket_name
                )
                current_parsed = parse_func(raw_data, context["execution_date"])
                parsed_data.extend(current_parsed)
            except Exception as error:
                logging.warning("Failed for %s. %s", object_name, error)
                if self.fail_on_errors:
                    raise error
        self.store(parsed_data, context)

    def store(self, data: list[dict[str, Any]], context: dict[str, Any]) -> None:
        raise NotImplementedError()


class BucketFilesToBigQueryOperator(BucketFilesToDatabase):
    def __init__(
        self,
        dataset_id: str,
        table_id: str,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.dataset_id = dataset_id
        self.table_id = table_id

    def store(self, data: list[dict[str, Any]], context: dict[str, Any]) -> None:
        bq_hook = BigQueryHook(gcp_conn_id=self.gcp_conn_id)
        bq_hook.insert_all(
            dataset_id=self.dataset_id, table_id=self.table_id, rows=data
        )


class BucketFilesToFirestoreOperator(BucketFilesToDatabase):
    def __init__(
        self,
        collection_id: str,
        key_column: str,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.collection_id = collection_id
        self.key_column = key_column

    def store(self, data: list[dict[str, Any]], context: dict[str, Any]) -> None:
        connection = BaseHook.get_connection(self.gcp_conn_id)
        sa_str = connection.extra_dejson["extra__google_cloud_platform__keyfile_dict"]
        sa_json = json.loads(sa_str)
        credentials = service_account.Credentials.from_service_account_info(sa_json)
        client = firestore.Client(credentials=credentials)
        batches = [data[item : item + 500] for item in range(0, len(data), 500)]
        collection = client.collection(self.collection_id)
        for batch_data in batches:
            batch = client.batch()
            for document in batch_data:
                batch.set(collection.document(document[self.key_column]), document)
            batch.commit()


def get_files(file_providers, file_providers_generator, context):
    if file_providers and file_providers_generator:
        raise ValueError("Provide either a file proivders or a a generator!")
    if file_providers:
        return file_providers.items()
    if file_providers_generator:
        return file_providers_generator(context)
    raise ValueError("No files provided!")
