import datetime as dt
import json

import requests


def download_data(execution_date: dt.datetime) -> bytes:
    """Downloads file with appropriate data from the exchangerate api.

    Args:
        execution_date: for what day

    Returns:
        bytes: result
    """
    endpoint = "https://api.exchangerate.host/{day}?base=PLN"
    endpoint = endpoint.format(day=execution_date.date())
    resp = requests.get(endpoint)
    resp.raise_for_status()
    data = resp.content
    return data


def parse_data(data: bytes, execution_date: dt.datetime):
    """Extracts data from file into correct format.

    Args:
        data: data from file
        execution_date: for what day data was downloaded

    Returns:
        final data
    """
    json_str = data.decode("utf-8")
    parsed = json.loads(json_str)
    results = []
    for currency, rate in parsed["rates"].items():
        results.append(
            {
                "date": parsed["date"],
                "base": parsed["base"],
                "currency": currency,
                "rate": rate,
            }
        )
    return results
