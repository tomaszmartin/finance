from app.scrapers import stocks


def test_getting_stocks(sample_equities):
    data, datetime = sample_equities
    result = stocks.get_stocks("equities", datetime)
    assert result is not None
    assert len(result) == 357489
    assert len(result) == len(data)


def test_parsing_equities(sample_equities):
    data, execution_date = sample_equities
    parsed = stocks.parse_stocks(data, execution_date)
    assert len(parsed) == 431
    assert parsed[0] == {
        "date": execution_date,
        "name": "06MAGNA",
        "isin_code": "PLNFI0600010",
        "currency": "PLN",
        "opening_price": 1.5500,
        "closing_price": 1.6300,
        "minimum_price": 1.5500,
        "maximum_price": 1.6900,
        "number_of_transactions": 195.0,
        "trade_volume": 197098.0,
        "turnover_value": 319910.0,
    }


def test_parsing_indices(sample_indices):
    data, execution_date = sample_indices
    parsed = stocks.parse_stocks(data, execution_date)
    from pprint import pprint

    pprint(parsed[0])
    assert len(parsed) == 43
    assert parsed[0] == {
        "date": execution_date,
        "name": "CEEplus",
        "isin_code": "PL9999998948",
        "currency": "PLN",
        "opening_price": 0.0,
        "closing_price": 993.95,
        "minimum_price": 0.0,
        "maximum_price": 0.0,
        "number_of_transactions": 0.0,
        "trade_volume": 0.0,
        "turnover_value": 0.0,
    }