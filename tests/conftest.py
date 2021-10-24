import datetime as dt
from typing import Tuple

from pytest import fixture


def _get_data(file_name: str) -> bytes:
    with open(f"tests/samples/{file_name}", "rb") as file:
        return file.read()


@fixture
def archive_equities() -> Tuple[bytes, dt.datetime]:
    return _get_data("equities.html"), dt.datetime(2021, 1, 4)


@fixture
def archive_indices() -> Tuple[bytes, dt.datetime]:
    return _get_data("indices.html"), dt.datetime(2021, 1, 4)


@fixture
def realtime_equities() -> Tuple[bytes, dt.datetime]:
    return _get_data("realtime_equities.html"), dt.datetime(2021, 7, 4)


@fixture
def realtime_indices() -> Tuple[bytes, dt.datetime]:
    return _get_data("realitme_indices.html"), dt.datetime(2021, 7, 4)


@fixture
def archive_currencies() -> Tuple[bytes, dt.datetime]:
    return _get_data("currencies.xml"), dt.datetime(2021, 7, 5)


@fixture
def company_info() -> Tuple[bytes, str, dt.datetime]:
    return _get_data("company_info.html"), "PL11BTS00015", dt.datetime(2021, 7, 4)


@fixture
def company_indicators() -> Tuple[bytes, str, dt.datetime]:
    return _get_data("company_indicators.html"), "PLGPW0000017", dt.datetime(2021, 7, 4)


@fixture
def coinapi_historical_resp() -> Tuple[bytes, str, dt.datetime]:
    return _get_data("coinapi_historical.json"), "BTC", dt.datetime(2013, 9, 28)


@fixture
def coinapi_realtime_resp() -> Tuple[bytes, str, dt.datetime]:
    return _get_data("coinapi_realtime.json"), "BTC", dt.datetime(2013, 9, 28)


@fixture
def currencies_data() -> Tuple[bytes, dt.datetime]:
    return _get_data("currencies.json"), dt.datetime(2021, 10, 9)
