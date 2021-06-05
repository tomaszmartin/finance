from app.scrapers import stocks


def test_getting_stocks(sample_equities):
    data, datetime = sample_equities
    result = stocks.get_stocks("equities", datetime)
    assert result is not None
    assert len(result) == 357489
    assert len(result) == len(data)


def test_parsing_stocks(sample_equities):
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
