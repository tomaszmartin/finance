import datetime as dt
from unittest import mock

import pytest

from app.scrapers.stocks import dimensions


def test_parsing_company_info(company_info):
    data, isin_code, execution_date = company_info
    parsed = dimensions.parse_data(data, isin_code, "info", execution_date)
    assert parsed == [
        {
            "date": execution_date.date(),
            "isin_code": isin_code,
            "abbreviation": "11B",
            "name": "11BIT",
            "full_name": "11 BIT STUDIOS SPÓŁKA AKCYJNA",
            "address": "UL. BRZESKA 2 03-737 WARSZAWA",
            "voivodeship": "mazowieckie",
            "email": "biuro@11bitstudios.com",
            "fax": "(22) 2502931",
            "phone": "(22) 250 29 10",
            "www": "www.11bitstudios.pl",
            "ceo": "Przemysław Marszał",
            "first_listing": "01.2011",
        }
    ]


def test_parsing_company_indicators(company_indicators):
    data, isin_code, execution_date = company_indicators
    parsed = dimensions.parse_data(data, isin_code, "indicators", execution_date)
    assert parsed == [
        {
            "date": execution_date.date(),
            "isin_code": "PLGPW0000017",
            "market": "Main",
            "sector": "exchanges and brokers",
            "dividend_yield": 0.056,
            "shares_issued": 41972000.0,
        }
    ]


def test_parsing_company_finance(company_finance):
    data, isin_code, execution_date = company_finance
    parsed = dimensions.parse_data(data, isin_code, "finance", execution_date)
    print(parsed)
    assert parsed == [
        {
            "assets": 16217065.0,
            "cash_flow_from_financing_activities": -172911.0,
            "cash_flow_from_investing_activities": -256939.0,
            "cash_flow_from_operating_activities": 1033300.0,
            "current_assets": 2858246.0,
            "current_liabilities": 962997.0,
            "current_ratio": None,
            "date": dt.date(2021, 7, 4),
            "debt_service_ratio": None,
            "depreciation": 380978.0,
            "ebitda": 1532113.0,
            "equity_shareholders_of_parent": 9116807.0,
            "financial_income": 118886.0,
            "isin_code": "LU2237380790",
            "net_cash_flow": 603450.0,
            "net_profit_loss_attributable_to_equity_holders_of_parent": 889882.0,
            "non_current_assets": 13358819.0,
            "non_current_liabilities": 6137261.0,
            "operating_profit_loss": 1151135.0,
            "other_operating_revenues": None,
            "period": "q1-q3 2021",
            "profit_loss_before_tax": 1106552.0,
            "profit_loss_on_sales": 1350070.0,
            "purchase_of_property_plant_equipment_and_intangible_assets": -256659.0,
            "quick_ratio": None,
            "return_on_assets": None,
            "return_on_equity": None,
            "revenues_from_sales": 3752193.0,
            "share_capital": 10233.0,
        },
    ]


@mock.patch("app.scrapers.stocks.dimensions.requests.get")
def test_downloading_info(mock_get):
    dimensions.get_data("ISIN_CODE", "info")
    mock_get.assert_called_with(
        "https://www.gpw.pl/ajaxindex.php"
        "?start=infoTab&format=html&action=GPWListaSp&gls_isin=ISIN_CODE&isin=ISIN_CODE&lang=EN"
    )


@mock.patch("app.scrapers.stocks.dimensions.requests.get")
def test_downloading_indicators(mock_get):
    dimensions.get_data("ISIN_CODE", "indicators")
    mock_get.assert_called_with(
        "https://www.gpw.pl/ajaxindex.php"
        "?start=indicatorsTab&format=html&action=GPWListaSp&gls_isin=ISIN_CODE&isin=ISIN_CODE&lang=EN"
    )


def test_downloading_worng_dim():
    with pytest.raises(ValueError):
        dimensions.get_data("ISIN_CODE", "ERROR")
