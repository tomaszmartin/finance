import datetime as dt
from app.scrapers import currencies


def test_parsing_currencies(archive_currencies):
    data, datetime = archive_currencies
    parsed = currencies.parse_currencies(data, datetime)
    assert len(parsed) == 2112
    assert parsed[0] == {
        "currency": "AUD",
        "to_eur": 1.5581,
        "date": dt.date(2021, 4, 7),
    }
    assert parsed[-1] == {
        "currency": "ZAR",
        "to_eur": 16.8813,
        "date": dt.date(2021, 7, 5),
    }
