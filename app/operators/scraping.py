import json
import logging
from typing import Any, Callable, Dict, List

from airflow.hooks.base_hook import BaseHook
from airflow.models.baseoperator import BaseOperator
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from google.oauth2 import service_account
from google.cloud import firestore


class FileToBucketOperator(BaseOperator):
    template_fields = ["bucket_name", "object_name"]

    def __init__(
        self,
        file_provider: Callable,
        gcp_conn_id: str,
        bucket_name: str,
        object_name: str,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.file_provider = file_provider
        self.gcp_conn_id = gcp_conn_id
        self.bucket_name = bucket_name
        self.object_name = object_name

    def execute(self, context):
        logging.info("Downloading data for %s", context["execution_date"])
        stocks_data = self.file_provider(context["execution_date"])
        storage_hook = GCSHook(google_cloud_storage_conn_id=self.gcp_conn_id)
        storage_hook.upload(
            self.bucket_name, object_name=self.object_name, data=stocks_data
        )


class BucketFileToDatabase(BaseOperator):
    template_fields = ["bucket_name", "object_name"]

    def __init__(
        self,
        parse_func: Callable,
        gcp_conn_id: str,
        bucket_name: str,
        object_name: str,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.parse_func = parse_func
        self.gcp_conn_id = gcp_conn_id
        self.bucket_name = bucket_name
        self.object_name = object_name

    def execute(self, context):
        logging.info("Parse and upload to BigQuery for %s", context["execution_date"])
        storage_hook = GCSHook(google_cloud_storage_conn_id=self.gcp_conn_id)
        data = storage_hook.download(
            object_name=self.object_name, bucket_name=self.bucket_name
        )
        parsed_data = self.parse_func(data, context["execution_date"])
        self.store(parsed_data, context)

    def store(self, data: List[Dict[str, Any]], context: Dict[str, Any]) -> None:
        raise NotImplementedError()


class BucketFileToBigQueryOperator(BucketFileToDatabase):
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


class BucketFileToFirestoreOperator(BucketFileToDatabase):
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
