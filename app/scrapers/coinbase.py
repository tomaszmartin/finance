"""Extracts data from a coinbase API."""
import datetime as dt
import pandas as pd

from airflow.providers.http.hooks.http import HttpHook

COINS = [
    "ADA",
    "BCH",
    "BTC",
    "BTT",
    "BNB",
    "BUSD",
    "DASH",
    "DOGE",
    "DOT",
    "EOS",
    "ETH",
    "ETC",
    "FLOW",
    "GRT",
    "ICP",
    "LTC",
    "LUNA",
    "SAND",
    "SOL",
    "TRX",
    "USDC",
    "XRP",
    "XLM",
]


def download_realtime(execution_date: dt.datetime, coin_symbol: str) -> bytes:
    """Downloads current prices for a specified coin.

    Args:
        coin_symbol: what coin should be downloaded, for example BTC
        execution_date: unused in this context

    Returns:
        bytes: result
    """
    hook = HttpHook("GET", http_conn_id="coinapi")
    endpoint = f"v1/quotes/BINANCE_SPOT_{coin_symbol.upper()}_USDT/current"
    resp = hook.run(endpoint)
    data = resp.content
    return data


def download_data(execution_date: dt.datetime, coin_symbol: str) -> bytes:
    """Downloads file with appropriate data from the CoinAPI.

    Args:
        coin_symbol: what coin should be downloaded, for example BTC
        execution_date: for what day

    Returns:
        bytes: result
    """
    next_day = execution_date + dt.timedelta(days=1)
    hook = HttpHook("GET", http_conn_id="coinapi")
    endpoint = "v1/exchangerate/{coin}/USD/history?period_id=1DAY&time_start={start}&time_end={end}"
    endpoint = endpoint.format(
        coin=coin_symbol.upper(), start=execution_date.date(), end=next_day.date()
    )
    resp = hook.run(endpoint)
    data = resp.content
    return data


def parse_data(data: bytes, execution_date: dt.datetime, coin_symbol: str = ""):
    """Extracts data from file into correct format.

    Args:
        data: data from file
        execution_date: for what day data was downloaded
        coin_symbol: what coin this data holds. It's not present in the file data.

    Raises:
        ValueError: when no coin is passed

    Returns:
        final data
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
    frame["date"] = execution_date.date()
    frame["coin"] = coin_symbol.upper()
    frame["base"] = "USD"
    return frame.to_dict("records")


def parse_realtime(data: bytes, execution_date: dt.datetime, coin_symbol: str = ""):
    """Extracts realtime data from file into correct format.

    Args:
        data: data from file
        execution_date: not used in this context
        coin_symbol: what coin this data holds. It's not present in the file data.

    Raises:
        ValueError: when no coin is passed

    Returns:
        final data
    """
    if not coin_symbol:
        raise ValueError("Need to specify coin!")
    frame = pd.read_json(data)
    frame["coin"] = coin_symbol.upper()
    frame["base"] = "USD"
    frame = frame.drop(
        columns=["symbol_id", "last_trade", "time_exchange", "time_coinapi"]
    )
    return frame.to_dict("records")
