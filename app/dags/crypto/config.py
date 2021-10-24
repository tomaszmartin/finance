"""Contains config for crypto dags."""
from functools import partial
from typing import Callable

from app.tools import datalake
from app.scrapers import coinapi


GCP_CONN_ID = "google_cloud"
BUCKET_NAME = "stocks_dl"
DATASET_ID = "cryptocurrencies"
HISTORY_TABLE_ID = "rates"
HISTORY_SCHEMA = [
    {"name": "date", "type": "DATE", "mode": "REQUIRED", "description": "Date."},
    {
        "name": "coin",
        "type": "STRING",
        "mode": "REQUIRED",
        "description": "Coin symbol.",
    },
    {
        "name": "base",
        "type": "STRING",
        "mode": "REQUIRED",
        "description": "Base for the price (in what currency the prices are).",
    },
    {
        "name": "open",
        "type": "FLOAT64",
        "mode": "REQUIRED",
        "description": "Opening price.",
    },
    {
        "name": "high",
        "type": "FLOAT64",
        "mode": "REQUIRED",
        "description": "Highest price.",
    },
    {
        "name": "low",
        "type": "FLOAT64",
        "mode": "REQUIRED",
        "description": "Lowest price.",
    },
    {
        "name": "close",
        "type": "FLOAT64",
        "mode": "REQUIRED",
        "description": "Closing price.",
    },
]


ARCHIVE_PROVIDERS: list[tuple[str, Callable]] = []
ARCHIVE_TRANSFORMERS: list[tuple[str, str, Callable]] = []
ARCHIVE_MASTER_FILES: list[str] = []
for coin in coinapi.COINS:
    args = {"process": "crypto", "dataset": "historical", "extension": "jsonl"}
    raw_file = datalake.raw(**args, prefix=coin)
    master_file = datalake.master(**args, prefix=coin)
    provider = partial(coinapi.download_data, coin_symbol=coin)
    parser = partial(coinapi.parse_data, coin_symbol=coin)

    ARCHIVE_PROVIDERS.append((raw_file, provider))
    ARCHIVE_TRANSFORMERS.append((raw_file, master_file, parser))
    ARCHIVE_MASTER_FILES.append(master_file)


REALTIME_PROVIDERS: list[tuple[str, Callable]] = []
REALTIME_TRANSFORMERS: list[tuple[str, str, Callable]] = []
REALTIME_MASTER_FILES: list[str] = []
for coin in coinapi.COINS:
    args = {"process": "crypto", "dataset": "realtime", "extension": "json"}
    raw_file = datalake.raw(**args, prefix=coin, with_timestamp=True)
    master_file = datalake.master(**args, prefix=coin, with_timestamp=True)
    provider = partial(coinapi.download_realtime, coin_symbol=coin)
    parser = partial(coinapi.parse_realtime, coin_symbol=coin)

    REALTIME_PROVIDERS.append((raw_file, provider))
    REALTIME_TRANSFORMERS.append((raw_file, master_file, parser))
    REALTIME_MASTER_FILES.append(master_file)
