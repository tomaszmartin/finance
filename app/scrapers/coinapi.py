"""Extracts data from a coinbase API."""
from typing import Any
import datetime as dt
import json

import pandas as pd
from airflow.providers.http.hooks.http import HttpHook

from app.scrapers import base

COINS = [
    "ADA",
    "BTC",
    "BTT",
    "BNB",
    "DASH",
    "DOGE",
    "ETH",
    "ETC",
    "LTC",
    "LUNA",
    "XLM",
]


# pylint: disable=unused-argument
def download_realtime(for_date: dt.datetime, coin_symbol: str) -> bytes:
    """Downloads current prices for a specified coin.

    Args:
        coin_symbol: what coin should be downloaded, for example BTC
        for_date: unused in this context

    Returns:
        bytes: result
    """
    hook = HttpHook("GET", http_conn_id="coinapi")
    endpoint = f"v1/quotes/BINANCE_SPOT_{coin_symbol.upper()}_USDT/current"
    response = hook.run(endpoint)
    return base.extract_content(response)


def download_data(for_date: dt.datetime, coin_symbol: str) -> bytes:
    """Downloads file with appropriate data from the CoinAPI.

    Args:
        coin_symbol: what coin should be downloaded, for example BTC
        for_date: for what day

    Returns:
        bytes: result
    """
    next_day = for_date + dt.timedelta(days=1)
    hook = HttpHook("GET", http_conn_id="coinapi")
    endpoint = "v1/exchangerate/{coin}/USD/history?period_id=1DAY&time_start={start}&time_end={end}"
    endpoint = endpoint.format(
        coin=coin_symbol.upper(), start=for_date.date(), end=next_day.date()
    )
    response = hook.run(endpoint)
    return base.extract_content(response)


def parse_data(
    data: bytes, for_date: dt.datetime, coin_symbol: str = ""
) -> list[dict[str, Any]]:
    """Extracts data from file into correct format.

    Args:
        data: data from file
        for_date: for what day data was downloaded
        coin_symbol: what coin this data holds. It's not present in the file data.

    Raises:
        ValueError: when no coin is passed

    Returns:
        Parsed data.
    """
    if not coin_symbol:
        raise ValueError("Need to specify coin!")
    frame = pd.read_json(data)
    frame = frame.rename(
        columns={
            "date": "time_close",
            "rate_open": "open",
            "rate_high": "high",
            "rate_low": "low",
            "rate_close": "close",
        }
    )
    frame = frame.drop(
        columns=["time_period_start", "time_period_end", "time_open", "time_close"]
    )
    frame["date"] = for_date.date()
    frame["coin"] = coin_symbol.upper()
    frame["base"] = "USD"
    return frame.to_dict("records")  # type: ignore


# pylint: disable=unused-argument
def parse_realtime(
    data: bytes, for_date: dt.datetime, coin_symbol: str = ""
) -> list[dict[str, Any]]:
    """Extracts realtime data from file into correct format.

    Args:
        data: data from file
        for_date: not used in this context
        coin_symbol: what coin this data holds. It's not present in the file data.

    Raises:
        ValueError: when no coin is passed

    Returns:
        final data
    """
    if not coin_symbol:
        raise ValueError("Need to specify coin!")
    json_data = json.loads(data)
    frame = pd.json_normalize(json_data)
    frame["coin"] = coin_symbol.upper()
    frame["base"] = "USD"
    frame = frame.drop(
        columns=[
            "symbol_id",
            "last_trade.time_exchange",
            "last_trade.time_coinapi",
            "last_trade.uuid",
            "last_trade.price",
            "last_trade.size",
            "last_trade.taker_side",
            "time_exchange",
            "time_coinapi",
        ]
    )
    return frame.to_dict("records")  # type: ignore
