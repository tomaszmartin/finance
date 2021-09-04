"""Helper functions for scraping and parsing data for GPW Polish Stock Exchange."""
import datetime as dt
import logging
from typing import Any

from bs4 import BeautifulSoup
import requests

from app import utils


def get_data(execution_date: dt.datetime, isin_code: str, fact_type: str) -> bytes:
    """Extracts facts about a company for a given tab.

    Args:
        isin_code: company isin code

    Returns:
        data about the company
    """
    allowed_facts = ["info", "indicators"]
    if fact_type not in allowed_facts:
        raise ValueError(f"Unknown fact type {fact_type}!")
    endpoint = f"https://www.gpw.pl/ajaxindex.php?start={fact_type}Tab&format=html&action=GPWListaSp&gls_isin={isin_code}&lang=EN"
    response = requests.get(endpoint)
    response.raise_for_status()
    return response.content


def parse_data(
    data: bytes, execution_date: dt.datetime, isin_code: str, fact_type: str
) -> list[dict[str, Any]]:
    soup = BeautifulSoup(data, "lxml")
    result: dict[str, Any] = {}
    main = soup.select(".footable")[0]
    rows = main.select("tr")
    for row in rows:
        key = row.select("th")[0].text.replace(":", "")
        value = row.select("td")[0].text.strip()
        key = utils.to_snake(key)
        result[key] = value
    if fact_type == "indicators":
        result = clean_indicators_data(result)
    if fact_type == "info":
        result = clean_info_data(result)
    result["isin_code"] = isin_code
    result["date"] = execution_date.date()
    return [result]


def clean_info_data(data: dict[str, Any]) -> dict[str, Any]:
    data["number_of_shares_issued"] = utils.to_float(
        data["number_of_shares_issued"], thusands_sep=" ", decimal_sep=","
    )
    data["market_value(mln_pln)"] = utils.to_float(
        data["market_value(mln_pln)"], thusands_sep=" ", decimal_sep=","
    )
    data["market_value"] = data.pop("market_value(mln_pln)")
    if not data["market_value"]:
        data["market_value"] = 0.0
    data["market_value"] = data["market_value"] * 1000000
    data = utils.rename(
        data,
        {
            "e-mail": "email",
            "date_of_first_listing": "first_listing",
            "company_address": "address",
        },
    )
    data = utils.drop(data, ["statute", "regs_/_s"])
    return data


def clean_indicators_data(data: dict[str, Any]) -> dict[str, Any]:
    data.pop("isin")
    for col in ["book_value", "market_value"]:
        data[f"{col}_(mln_pln)"] = utils.to_float(data[f"{col}_(mln_pln)"])
        data[col] = data.pop(f"{col}_(mln_pln)")
        if not data[col]:
            data[col] = 0.0
        data[col] = data[col] * 1000000
    data = utils.rename(data, {"market/_segment": "market"})
    data["dividend_yield"] = data.pop("dividend_yield_(%)").replace("---", "")
    data["number_of_shares_issued"] = utils.to_float(data["number_of_shares_issued"])
    data["dividend_yield"] = utils.to_float(data["dividend_yield"])
    data["pbv"] = utils.to_float(data.pop("p/bv"))
    data["pe"] = utils.to_float(data.pop("p/e"))
    if not data["dividend_yield"]:
        data["dividend_yield"] = 0.0
    data["dividend_yield"] = round(data["dividend_yield"] / 100.0, 5)
    return data
