from app.scrapers.stocks import prices


def test_parsing_archive_equities(archive_equities):
    data, execution_date = archive_equities
    parsed = prices.parse_archive(data, execution_date)
    assert len(parsed) == 431
    assert parsed[0] == {
        "date": execution_date.date(),
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


def test_parsing_archive_indices(archive_indices):
    data, execution_date = archive_indices
    parsed = prices.parse_archive(data, execution_date)
    assert len(parsed) == 43
    assert parsed[0] == {
        "name": "CEEplus",
        "isin_code": "PL9999998948",
        "currency": "PLN",
        "opening_price": 0.0,
        "maximum_price": 0.0,
        "minimum_price": 0.0,
        "closing_price": 993.95,
        "number_of_transactions": 0.0,
        "trade_volume": 0.0,
        "turnover_value": 0.0,
        "date": execution_date.date(),
    }


def test_parsing_realtime_equities(realtime_equities):
    data, execution_date = realtime_equities
    parsed = prices.parse_realtime(data, execution_date)
    assert len(parsed) == 392
    assert parsed[0] == {
        "name": "06MAGNA",
        "shortcut": "06N",
        "isin_code": "PLNFI0600010",
        "currency": "PLN",
        "last_transaction_time": "17:00:00",
        "reference_price": 2.3400,
        "theoretical_open_price": None,
        "open": 2.3350,
        "low": 2.3350,
        "high": 2.4750,
        "closing": 2.4750,
        "timestamp": 1625349600.0,
        "corporate_actions": "",
    }


def test_parsing_realtime_indices(realtime_indices):
    data, execution_date = realtime_indices
    parsed = prices.parse_realtime(data, execution_date)
    assert len(parsed) == 43
    from pprint import pprint

    pprint(parsed[0])
    assert parsed[0] == {
        "index": "WIG20",
        "number_of_companies": 20.0,
        "time": "17:15:01",
        "theoretical_index_value": None,
        "open": 2258.56,
        "min_price": 2249.91,
        "max_price": 2269.40,
        "value": 2252.18,
        "isin_code": "PL9999999987",
        "cumulated_value": 562248170.0,
        "timestamp": 1625349600.0,
    }