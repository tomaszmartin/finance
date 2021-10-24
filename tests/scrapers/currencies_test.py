import datetime as dt
from unittest import mock

from app.scrapers import currencies


def test_currencies_scraping(currencies_data):
    data, execution_date = currencies_data
    parsed = currencies.parse_data(data, execution_date)
    assert len(parsed) == 170
    assert parsed[0:4] == [
        {"base": "PLN", "currency": "AED", "date": "2021-10-09", "rate": 0.923766},
        {"base": "PLN", "currency": "AFN", "date": "2021-10-09", "rate": 22.796918},
        {"base": "PLN", "currency": "ALL", "date": "2021-10-09", "rate": 26.386923},
        {"base": "PLN", "currency": "AMD", "date": "2021-10-09", "rate": 121.894925},
    ]


@mock.patch("app.scrapers.currencies.requests.get")
def test_downloading_historical(mocked_get):
    currencies.download_data(dt.datetime(2021, 1, 1))
    mocked_get.assert_called_with("https://api.exchangerate.host/2021-01-01?base=PLN")


@mock.patch("app.scrapers.currencies.requests.get")
def test_downloading_realtime(mocked_get):
    today = dt.date.today()
    currencies.download_data(dt.datetime(2021, 1, 1), realtime=True)
    mocked_get.assert_called_with(f"https://api.exchangerate.host/{today}?base=PLN")