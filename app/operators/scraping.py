import json
import logging
from typing import Any, Callable, Dict, List

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
        file_providers: dict[str, Callable],
        gcp_conn_id: str,
        bucket_name: str,
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
        self.file_providers = file_providers
        self.gcp_conn_id = gcp_conn_id
        self.bucket_name = bucket_name

    def execute(self, context):
        for object_name, file_provider in self.file_providers.items():
            data = file_provider(context["execution_date"])
            storage_hook = GCSHook(google_cloud_storage_conn_id=self.gcp_conn_id)
            storage_hook.upload(self.bucket_name, object_name=object_name, data=data)


class BucketFilesToDatabase(BaseOperator):
    template_fields = ["file_parsers"]

    def __init__(
        self,
        file_parsers: dict[str, Callable],
        gcp_conn_id: str,
        bucket_name: str,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.file_parsers = file_parsers
        self.gcp_conn_id = gcp_conn_id
        self.bucket_name = bucket_name

    def execute(self, context):
        logging.info("Parse and upload to BigQuery for %s", context["execution_date"])
        parsed_data = []
        for object_name, parse_func in self.file_parsers.items():
            storage_hook = GCSHook(google_cloud_storage_conn_id=self.gcp_conn_id)
            raw_data = storage_hook.download(
                object_name=object_name, bucket_name=self.bucket_name
            )
            current_parsed = parse_func(raw_data, context["execution_date"])
            parsed_data.extend(current_parsed)
        self.store(parsed_data, context)

    def store(self, data: List[Dict[str, Any]], context: Dict[str, Any]) -> None:
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

    def store(self, data: List[Dict[str, Any]], context: Dict[str, Any]) -> None:
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

    def store(self, data: List[Dict[str, Any]], context: Dict[str, Any]) -> None:
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
