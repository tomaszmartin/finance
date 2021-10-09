"""Keeps config information for the DAG."""


"""Contains config for currencies dags."""

GCP_CONN_ID = "google_cloud"
BUCKET_NAME = "stocks_dl"
DATASET_ID = "currencies"
HISTORY_TABLE_ID = "rates"
HISTORY_SCHEMA = [
    {"name": "date", "type": "DATE", "mode": "REQUIRED", "description": "Date."},
    {
        "name": "currency",
        "type": "STRING",
        "mode": "REQUIRED",
        "description": "Currency code in ISO 4217.",
    },
    {
        "name": "base",
        "type": "STRING",
        "mode": "REQUIRED",
        "description": "Base currency for the price (in what currency the prices are).",
    },
    {
        "name": "rate",
        "type": "FLOAT64",
        "mode": "REQUIRED",
        "description": "Price of the currnecy in base currency.",
    },
]