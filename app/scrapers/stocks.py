import datetime as dt
from typing import Dict, List

import requests
from bs4 import BeautifulSoup

from app import utils


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


def parse_stocks(stocks_data: bytes):
    soup = BeautifulSoup(stocks_data, "lxml")
    column_names = get_column_names(soup)
    main = soup.select(".mainContainer")[0]
    rows = main.select("tr")
    records = [parse_row(row, column_names) for row in rows]
    records = [rec for rec in records if rec]
    return records


def get_column_names(soup: BeautifulSoup) -> List[str]:
    header = soup.find("thead")
    columns_names = [utils.to_snake(tag.text) for tag in header.find_all("th")]
    return columns_names


def parse_row(row: BeautifulSoup, column_names: List[str]) -> Dict[str, str]:
    metrics = [
        "closing_price",
        "opening_price",
        "maximum_price",
        "minimum_price",
        "trade_volume_(#)",
        "number_of_transactions",
        "turnover_value_(thou.)",
    ]
    fields = row.find_all("td")
    record = {name: field.text.strip() for name, field in zip(column_names, fields)}
    if not record:
        return {}
    for metric in metrics:
        record[metric] = to_float(record[metric])
    record["trade_volume"] = record.pop("trade_volume_(#)")
    record["turnover_value"] = record.pop("turnover_value_(thou.)") * 1000
    record.pop("%_price_change")
    return record


def to_float(value: str) -> float:
    value = value.replace(",", "")
    return float(value)
