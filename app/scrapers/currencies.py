import datetime as dt
from typing import Any, Dict, List

from bs4 import BeautifulSoup
import requests

from app import utils


def get_currencies(execution_date: dt.date) -> bytes:
    url = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-hist-90d.xml"
    resp = requests.get(url)
    resp.raise_for_status()
    return resp.content


def parse_currencies(data: bytes, datetime: dt.datetime):
    soup = BeautifulSoup(data, "xml")
    results: List[Dict[str, Any]] = []
    for day in soup.find("Cube").find_all("Cube", recursive=False):
        time = dt.datetime.strptime(day["time"], "%Y-%m-%d").date()
        rates = [
            {
                "currency": rate["currency"],
                "to_eur": utils.to_float(rate["rate"]),
                "date": time,
            }
            for rate in day.find_all("Cube")
        ]
        rates.append({"currency": "EUR", "to_eur": 1.0, "date": time})
        results.extend(rates)

    results = sorted(results, key=lambda item: (item["date"], item["currency"]))
    return results
