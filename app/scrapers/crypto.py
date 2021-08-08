import datetime as dt
import pandas as pd

from airflow.providers.http.hooks.http import HttpHook


def download_data(coin_symbol: str, execution_date: dt.datetime) -> bytes:
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
        ValueError: when no coin_symbol is passed

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
