from typing import Callable
from app.scrapers import crypto
from functools import partial

ARCHIVE_PROVIDERS: dict[str, Callable] = {
    f"crypto/archive/{{execution_date.year}}/{coin}/"
    + "{{ds}}.json": partial(crypto.download_data, coin)
    for coin in crypto.COINS
}
ARCHIVE_PARSERS: dict[str, Callable] = {
    f"crypto/archive/{{execution_date.year}}/{coin}/"
    + "{{ds}}.json": partial(crypto.parse_data, coin=coin)
    for coin in crypto.COINS
}

REALTIME_PROVIDERS: dict[str, Callable] = {
    f"crypto/realtime/{coin}.json": partial(crypto.download_realtime, coin)
    for coin in crypto.COINS
}
REALTIME_PARSERS: dict[str, Callable] = {
    f"crypto/realtime/{coin}.json": partial(crypto.parse_realtime, coin=coin)
    for coin in crypto.COINS
}
