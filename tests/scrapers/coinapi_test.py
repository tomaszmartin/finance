import datetime as dt
from unittest import mock

import pytest

from app.scrapers import coinapi


def test_parsing_historical(coinapi_historical_resp):
    data, symbol, for_date = coinapi_historical_resp
    parsed = coinapi.parse_data(data, for_date=for_date, coin_symbol=symbol)
    assert parsed == [
        {
            "date": dt.date(2013, 9, 28),
            "base": "USD",
            "coin": "BTC",
            "open": 53971.35447991789,
            "high": 55398.119889263166,
            "low": 53772.465713124686,
            "close": 54969.22831698869,
        }
    ]


@pytest.mark.parametrize("symbol", ["", None])
def test_historical_no_coin(symbol, coinapi_historical_resp):
    data, _, for_date = coinapi_historical_resp
    with pytest.raises(ValueError):
        coinapi.parse_data(data, for_date=for_date, coin_symbol=symbol)


def test_parsing_realtime(coinapi_realtime_resp):
    data, symbol, for_date = coinapi_realtime_resp
    parsed = coinapi.parse_realtime(data, for_date=for_date, coin_symbol=symbol)
    assert parsed == [
        {
            "ask_price": 55106.04,
            "ask_size": 0.29193,
            "base": "USD",
            "bid_price": 55106.03,
            "bid_size": 0.71707,
            "coin": "BTC",
        },
    ]


@pytest.mark.parametrize("symbol", ["", None])
def test_realtime_no_coin(symbol, coinapi_realtime_resp):
    data, _, for_date = coinapi_realtime_resp
    with pytest.raises(ValueError):
        coinapi.parse_realtime(data, for_date=for_date, coin_symbol=symbol)


@mock.patch("app.scrapers.coinapi.HttpHook.run")
def test_downloading_realtime(mock_hook_run):
    coinapi.download_realtime(dt.datetime(2021, 1, 1), "BTC")
    mock_hook_run.assert_called_with("v1/quotes/BINANCE_SPOT_BTC_USDT/current")


@mock.patch("app.scrapers.coinapi.HttpHook.run")
def test_downloading_historical(mock_hook_run):
    coinapi.download_data(dt.datetime(2021, 1, 1), "BTC")
    mock_hook_run.assert_called_with(
        "v1/exchangerate/BTC/USD/history"
        "?period_id=1DAY"
        "&time_start=2021-01-01"
        "&time_end=2021-01-02"
    )
