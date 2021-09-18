"""Contains common constants for crypto dags."""
from functools import partial
from typing import Callable

from app.tools import datalake
from app.scrapers import coinbase


ARCHIVE_PROVIDERS: list[tuple[str, Callable]] = []
ARCHIVE_TRANSFORMERS: list[tuple[str, str, Callable]] = []
ARCHIVE_MASTER_FILES: list[str] = []
for coin in coinbase.COINS:
    args = {"process": "crypto", "dataset": "historical", "extension": "jsonl"}
    raw_file = datalake.raw(**args, prefix=coin)
    master_file = datalake.master(**args, prefix=coin)
    provider = partial(coinbase.download_data, coin_symbol=coin)
    parser = partial(coinbase.parse_data, coin_symbol=coin)

    ARCHIVE_PROVIDERS.append((raw_file, provider))
    ARCHIVE_TRANSFORMERS.append((raw_file, master_file, parser))
    ARCHIVE_MASTER_FILES.append(master_file)


REALTIME_PROVIDERS: list[tuple[str, Callable]] = []
REALTIME_TRANSFORMERS: list[tuple[str, str, Callable]] = []
REALTIME_MASTER_FILES: list[str] = []
for coin in coinbase.COINS:
    args = {"process": "crypto", "dataset": "realtime", "extension": "json"}
    raw_file = datalake.raw(**args, prefix=coin, with_timestamp=True)
    master_file = datalake.master(**args, prefix=coin, with_timestamp=True)
    provider = partial(coinbase.download_realtime, coin_symbol=coin)
    parser = partial(coinbase.parse_realtime, coin_symbol=coin)

    REALTIME_PROVIDERS.append((raw_file, provider))
    REALTIME_TRANSFORMERS.append((raw_file, master_file, parser))
    REALTIME_MASTER_FILES.append(master_file)
