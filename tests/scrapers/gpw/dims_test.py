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
            "market_value": 1099600000.0,
            "number_of_shares_issued": 2363711.0,
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
            "book_value": 963260000.0,
            "market_value": 1888740000.0,
            "dividend_yield": 0.056,
            "number_of_shares_issued": 41972000.0,
            "pbv": 1.96,
            "pe": 11.70,
        }
    ]


@mock.patch("app.scrapers.stocks.dimensions.requests.get")
def test_downloading_info(mock_get):
    dimensions.get_data("ISIN_CODE", "info")
    mock_get.assert_called_with(
        "https://www.gpw.pl/ajaxindex.php"
        "?start=infoTab&format=html&action=GPWListaSp&gls_isin=ISIN_CODE&lang=EN"
    )


@mock.patch("app.scrapers.stocks.dimensions.requests.get")
def test_downloading_indicators(mock_get):
    dimensions.get_data("ISIN_CODE", "indicators")
    mock_get.assert_called_with(
        "https://www.gpw.pl/ajaxindex.php"
        "?start=indicatorsTab&format=html&action=GPWListaSp&gls_isin=ISIN_CODE&lang=EN"
    )


def test_downloading_worng_dim():
    with pytest.raises(ValueError):
        dimensions.get_data("ISIN_CODE", "ERROR")
