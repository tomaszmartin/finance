from app.scrapers.stocks import facts


def test_parsing_company_info(company_info):
    data, isin_code, execution_date = company_info
    parsed = facts.parse_data(data, execution_date, isin_code, "info")
    assert parsed == {
        "date": execution_date.date(),
        "isin_code": isin_code,
        "abbreviation": "11B",
        "ceo": "Przemysław Marszał",
        "company_address": "UL. BRZESKA 2 03-737 WARSZAWA",
        "date_of_first_listing": "01.2011",
        "e-mail": "biuro@11bitstudios.com",
        "fax": "(22) 2502931",
        "full_name": "11 BIT STUDIOS SPÓŁKA AKCYJNA",
        "market_value": 1099600000.0,
        "name": "11BIT",
        "number_of_shares_issued": 2363711.0,
        "phone": "(22) 250 29 10",
        "voivodeship": "mazowieckie",
        "www": "www.11bitstudios.pl",
    }


def test_parsing_company_indicators(company_indicators):
    data, isin_code, execution_date = company_indicators
    parsed = facts.parse_data(data, execution_date, isin_code, "indicators")
    assert parsed == {
        "date": execution_date.date(),
        "book_value": 963260000.0,
        "dividend_yield": 0.056,
        "isin_code": "PLGPW0000017",
        "market": "Main",
        "market_value": 1888740000.0,
        "number_of_shares_issued": 41972000.0,
        "p/bv": 1.96,
        "p/e": 11.70,
        "sector": "exchanges and brokers",
    }
