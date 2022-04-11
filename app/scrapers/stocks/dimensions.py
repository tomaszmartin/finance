"""Helper functions for scraping and parsing data for GPW Polish Stock Exchange."""
import datetime as dt
from typing import Any, Optional

import requests
from bs4 import BeautifulSoup

from app.scrapers import base
from app.tools import utils


def get_data(isin_code: str, fact_type: str) -> bytes:
    """Extracts facts about a company for a given tab.

    Args:
        isin_code: company isin code
        fact_type: what type of fact to download

    Returns:
        data about the company

    Raises:
        ValueError: When unknown fact type is used.
    """
    tabs = {
        "info": "infoTab",
        "indicators": "indicatorsTab",
        "finance": "showNotoria",
    }
    if fact_type not in tabs.keys():
        raise ValueError(f"Unknown fact type {fact_type}!")
    endpoint = (
        f"https://www.gpw.pl/ajaxindex.php?start={tabs[fact_type]}"
        f"&format=html&action=GPWListaSp&gls_isin={isin_code}&isin={isin_code}&lang=EN"
    )
    response = requests.get(endpoint)
    return base.extract_content(response)


def parse_data(
    data: bytes, isin_code: str, fact_type: str, execution_date: dt.datetime
) -> Optional[list[dict[str, Any]]]:
    """Parses data in html into a list of dicts."""
    soup = BeautifulSoup(data, "lxml")
    result: dict[str, Any] = {}
    if _empty(soup):
        return None
    main = soup.select(".footable")[0]
    rows = main.select("tr")
    result["isin_code"] = isin_code
    result["date"] = execution_date.date()
    for row in rows:
        key = row.select("th")[0].text.replace(":", "")
        value = row.select("td")[0].text.strip()
        key = utils.to_snake(key)
        result[key] = value
    if fact_type == "indicators":
        result = _clean_indicators_data(result)
    if fact_type == "info":
        result = _clean_info_data(result)
    if fact_type == "finance":
        result = _clean_finance_data(result)
        result["period"] = soup.select("h3")[0].text.strip().lower()
    return [result]


def _empty(soup: BeautifulSoup) -> bool:
    main = soup.select(".footable")
    if not main:
        if soup.text.strip().lower() == "not available":
            return True
    return False


def _clean_info_data(data: dict[str, Any]) -> dict[str, Any]:
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
    data = utils.drop(
        data, ["statute", "regs_/_s", "number_of_shares_issued", "market_value"]
    )
    return data


def _clean_indicators_data(data: dict[str, Any]) -> dict[str, Any]:
    data = utils.drop(
        data,
        [
            "book_value_(mln_pln)",
            "market_value_(mln_pln)",
            "isin",
            "p/bv",
            "p/e",
        ],
    )
    data = utils.rename(data, {"market/_segment": "market"})
    data["dividend_yield"] = data.pop("dividend_yield_(%)").replace("---", "")
    data["shares_issued"] = utils.to_float(data.pop("number_of_shares_issued"))
    data["dividend_yield"] = utils.to_float(data["dividend_yield"])
    if not data["dividend_yield"]:
        data["dividend_yield"] = 0.0
    data["dividend_yield"] = round(data["dividend_yield"] / 100.0, 5)
    return data


def _clean_finance_data(data: dict[str, Any]) -> dict[str, Any]:
    columns = {
        "revenues_from_sales": "revenues_from_sales",
        "profit/loss_on_sales": "profit_loss_on_sales",
        "operating_profit/loss": "operating_profit_loss",
        "financial_income": "financial_income",
        "profit/loss_before_tax": "profit_loss_before_tax",
        "net_profit/loss_attributable_to_equity_holders_of_the_parent": (
            "net_profit_loss_attributable_to_equity_holders_of_parent"
        ),
        "depreciation": "depreciation",
        "assets": "assets",
        "non-current_assets": "non_current_assets",
        "current_assets": "current_assets",
        "equity_shareholders_of_the_parent": "equity_shareholders_of_parent",
        "share_capital": "share_capital",
        "non-current_liabilities": "non_current_liabilities",
        "current_liabilities": "current_liabilities",
        "cash_flow_from_operating_activities": "cash_flow_from_operating_activities",
        "cash_flow_from_investing_activities": "cash_flow_from_investing_activities",
        "purchase_of_property,_plant,_equipment_and_intangible_assets": (
            "purchase_of_property_plant_equipment_and_intangible_assets"
        ),
        "cash_flow_from_financing_activities": "cash_flow_from_financing_activities",
        "net_cash_flow": "net_cash_flow",
        "ebitda": "ebitda",
        "current_ratio": "current_ratio",
        "quick_ratio": "quick_ratio",
        "debt_service_ratio": "debt_service_ratio",
        "return_on_assets_(roa)": "return_on_assets",
        "return_on_equity_(roe)": "return_on_equity",
        "other_operating_revenues": "other_operating_revenues",
    }
    data = utils.rename(data, columns)
    for _, column in columns.items():
        if column in data:
            data[column] = utils.to_float(data[column])
        else:
            data[column] = None
    return data
