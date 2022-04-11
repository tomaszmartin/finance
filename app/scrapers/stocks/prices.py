"""Helper functions for scraping and parsing data for GPW Polish Stock Exchange."""
import datetime as dt
import urllib.parse as urlparse
from typing import Any, Dict, List
from urllib.parse import parse_qs

import requests
from bs4 import BeautifulSoup

from app.tools import utils
from app.scrapers import base


def get_archive(instrument: str, execution_date: dt.date) -> bytes:
    """Extracts archive data by extracting html of the page.

    Args:
        instrument: equities od indices
        execution_date (dt.date): what day

    Returns:
        Website content.

    Raise:
        ValueError when extracted data in wrong format.
    """
    type_map = {"equities": "10", "indices": "1"}
    params = {
        "type": type_map[instrument],
        "instrument": "",
        "date": execution_date.strftime("%d-%m-%Y"),
    }
    url = "https://www.gpw.pl/price-archive-full"
    response = requests.get(url, params=params)
    return base.extract_content(response)


# pylint: disable=unused-argument
def get_realtime(instrument: str, execution_date: dt.date) -> bytes:
    """Extracts current data by extracting html of the page.

    Args:
        instrument: equities od indices
        execution_date (dt.date): what day (unused here, but needed for compatibility)

    Returns:
        website contents
    """
    type_map = {
        "equities": (
            "https://www.gpw.pl/ajaxindex.php?action=GPWQuotations"
            "&start=showTable&tab=search&lang=EN&full=1&format=html"
        ),
        "indices": (
            "https://gpwbenchmark.pl/ajaxindex.php?action=GPWIndexes"
            "&start=showTable&tab=all&lang=EN&format=html"
        ),
    }
    response = requests.get(type_map[instrument])
    return base.extract_content(response)


def parse_archive(raw_data: bytes, execution_date: dt.datetime) -> list[dict[str, Any]]:
    """Parses archive html data.

    Args:
        raw_data: Website data in html.
        execution_date: For what date data is parsed.

    Returns:
        Parsed data.
    """
    soup = BeautifulSoup(raw_data, "lxml")
    column_names = get_column_names(soup)
    main = soup.select(".table.footable")[0]
    rows = main.select("tr")
    records: List[Dict[str, Any]] = []
    records = [parse_row(row, column_names) for row in rows]
    records = [rec for rec in records if rec]
    for record in records:
        record["date"] = execution_date.date()
    return records


def parse_realtime(
    raw_data: bytes, execution_date: dt.datetime
) -> list[dict[str, Any]]:
    """Parses current html data.

    raw_data: Website data in html.
        execution_date: For what date data is parsed.

    Returns:
        Parsed data.
    """
    soup = BeautifulSoup(raw_data, "lxml")
    main = soup.select(".table")[-1]
    column_names = get_column_names(main)[:12]
    rows = main.select("tr:not(.footable-group-row):not(.summary)")[1:]
    records: list[dict[str, Any]] = []
    records = [parse_realtime_row(row, column_names) for row in rows]
    records = [rec for rec in records if rec]
    for record in records:
        record["timestamp"] = execution_date.timestamp()
    return records


def get_column_names(soup: BeautifulSoup) -> list[str]:
    """Extracts column names from the soup.

    Args:
        soup: website content

    Returns:
        list of columns
    """
    header = soup.find("thead")
    columns_names = [utils.to_snake(tag.text) for tag in header.find_all("th")]
    columns_names = [col if col != "currency" else "base" for col in columns_names]
    return columns_names


def parse_realtime_row(row: BeautifulSoup, column_names: List[str]) -> Dict[str, str]:
    """Parses row of relitime data.

    Args:
        row: row data
        column_names: list of column names to be extracted

    Returns:
        dict with row data
    """
    link = row.find_all("a")[1]
    url_address = "https://www.gpw.pl/" + link["href"]
    parsed = urlparse.urlparse(url_address)
    isin_code = parse_qs(parsed.query)["isin"][0]
    record = parse_row(row, column_names)
    record["isin_code"] = isin_code
    record.pop("")
    return record


def parse_row(row: BeautifulSoup, column_names: List[str]) -> Dict[str, str]:
    """Parses row of data.

    Args:
        row: row data
        column_names: list of column names to be extracted

    Returns:
        dict with row data
    """
    metrics = [
        "closing_price",
        "opening_price",
        "maximum_price",
        "minimum_price",
        "max_price",
        "min_price",
        "trade_volume_(#)",
        "number_of_transactions",
        "turnover_value_(thou.)",
        "cumulated__value_(thous_pln)",
        "cumulated_value(pln_thous.)",
        "best_ask",
        "best_bid",
        "cumulated_volume",
        "theoretical_open_price",
        "theoretical__index__value",
        "high",
        "last_/__closing",
        "last_volume",
        "limit",
        "low",
        "no._of_orders",
        "number_of_trades",
        "open",
        "reference_price",
        "volume",
        "value",
        "number_of_companies",
    ]
    fields = row.find_all("td")
    record = {name: field.text.strip() for name, field in zip(column_names, fields)}
    columns = list(record.keys())
    if not record:
        return {}
    for metric in metrics:
        if metric in columns:
            record[metric] = utils.to_float(record[metric])

    thou_columns = [
        "turnover_value_(thou.)",
        "cumulated_value(pln_thous.)",
        "cumulated__value_(thous_pln)",
    ]
    for col in thou_columns:
        if col in columns:
            if record[col]:  # if not None
                record[col] = record[col] * 1000.0

    rename_columns = {
        "trade_volume_(#)": "trade_volume",
        "last_/__closing": "closing",
        "turnover_value_(thou.)": "turnover_value",
        "cumulated_value(pln_thous.)": "cumulated_value",
        "theoretical__index__value": "theoretical_index_value",
        "cumulated__value_(thous_pln)": "cumulated_value",
    }
    record = utils.rename(record, rename_columns)
    record = utils.drop(record, ["%_price_change", "%_change", "%_opened_portfolio"])
    return record
