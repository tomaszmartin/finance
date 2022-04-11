"""Enables to extract data from exchangarate API."""
import datetime as dt
import json
from typing import Any

import requests

from app.scrapers import base


def download_data(execution_date: dt.datetime, realtime: bool = False) -> bytes:
    """Downloads file with appropriate data from the exchangerate api.

    Args:
        execution_date: for what day
        realtime: if should download realtime data

    Returns:
        bytes: result
    """
    day = execution_date.date()
    if realtime:
        day = dt.date.today()
    endpoint = f"https://api.exchangerate.host/{day}?base=PLN"
    response = requests.get(endpoint)
    return base.extract_content(response)


# pylint: disable=unused-argument
def parse_data(raw_data: bytes, execution_date: dt.datetime) -> list[dict[str, Any]]:
    """Extracts data from file into correct format.

    Args:
        raw_data: data from file
        execution_date: for what day data was downloaded

    Returns:
        final data
    """
    json_str = raw_data.decode("utf-8")
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
