import datetime as dt
from sqlite3 import connect
from typing import Tuple
from unittest import mock

import sqlalchemy
from pytest import fixture


def _get_data(file_name: str) -> bytes:
    with open(f"tests/samples/{file_name}", "rb") as file:
        return file.read()


EXECUTION_DATE = dt.datetime(2021, 1, 1, tzinfo=dt.timezone.utc)


@fixture
def archive_equities() -> Tuple[bytes, dt.datetime]:
    return (_get_data("equities.html"), EXECUTION_DATE)


@fixture
def archive_indices() -> Tuple[bytes, dt.datetime]:
    return _get_data("indices.html"), EXECUTION_DATE


@fixture
def realtime_equities() -> Tuple[bytes, dt.datetime]:
    return _get_data("realtime_equities.html"), EXECUTION_DATE


@fixture
def realtime_indices() -> Tuple[bytes, dt.datetime]:
    return _get_data("realitme_indices.html"), EXECUTION_DATE


@fixture
def archive_currencies() -> Tuple[bytes, dt.datetime]:
    return _get_data("currencies.xml"), EXECUTION_DATE


@fixture
def company_info() -> Tuple[bytes, str, dt.datetime]:
    return _get_data("company_info.html"), "PL11BTS00015", EXECUTION_DATE


@fixture
def company_finance() -> Tuple[bytes, str, dt.datetime]:
    return _get_data("company_finance.html"), "LU2237380790", EXECUTION_DATE


@fixture
def company_indicators() -> Tuple[bytes, str, dt.datetime]:
    return _get_data("company_indicators.html"), "PLGPW0000017", EXECUTION_DATE


@fixture
def coinapi_historical_resp() -> Tuple[bytes, str, dt.datetime]:
    return _get_data("coinapi_historical.json"), "BTC", EXECUTION_DATE


@fixture
def coinapi_realtime_resp() -> Tuple[bytes, str, dt.datetime]:
    return _get_data("coinapi_realtime.json"), "BTC", EXECUTION_DATE


@fixture
def currencies_data() -> Tuple[bytes, dt.datetime]:
    return _get_data("currencies.json"), EXECUTION_DATE


@fixture
def mock_coinapi_hook():
    with mock.patch("app.scrapers.coinapi.HttpHook", autospec=True) as mock_hook:
        yield mock_hook.return_value


@fixture
def db_conn():
    engine = sqlalchemy.create_engine("sqlite://")
    connection = engine.connect()
    yield connection
    connection.close()


@fixture
def db_session(db_conn):
    db_session = db_conn.connect()
    yield db_session
    db_conn.close()
