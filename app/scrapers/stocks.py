import datetime as dt
import requests


def get_stocks(instrument: str, execution_date: dt.date) -> bytes:
    type_map = {"equities": "10", "indices": "1"}
    params = {
        "type": type_map[instrument],
        "instrument": "",
        "date": execution_date.strftime("%d-%m-%Y"),
    }
    url = f"https://www.gpw.pl/price-archive-full"
    resp = requests.get(url, params=params)
    resp.raise_for_status()
    return resp.content
