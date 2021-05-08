import datetime as dt

import requests
import logging

from app import utils, logger


def download(date: dt.date, category: str):
    logging.info("Downloading %s data for %s", category, date)
    type_map = {"equities": "10", "indices": "1"}
    params = {
        "type": type_map[category],
        "instrument": "",
        "date": date.strftime("%d-%m-%Y"),
    }
    url = f"https://www.gpw.pl/price-archive-full"
    path = f"data/raw/{category}/{date.year}/{date.strftime('%Y-%m-%d')}.html"
    resp = requests.get(url, params=params)
    with open(path, "wb") as file:
        file.write(resp.content)


if __name__ == "__main__":
    stock_launch = dt.date(1991, 4, 16)
    year = 2008
    since = dt.date(year, 1, 1)
    until = dt.date(year, 12, 31)
    for day in utils.days_between(since, until):
        download(day, "equities")
        download(day, "indices")
