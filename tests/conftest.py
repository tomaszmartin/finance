import datetime as dt
from typing import Tuple

from pytest import fixture


@fixture
def sample_equities() -> Tuple[bytes, dt.datetime]:
    with open("tests/samples/equities.html", "rb") as file:
        return file.read(), dt.datetime(2021, 1, 4)


@fixture
def sample_indices() -> Tuple[bytes, dt.datetime]:
    with open("tests/samples/indices.html", "rb") as file:
        return file.read(), dt.datetime(2021, 1, 4)


@fixture
def realtime_equities() -> Tuple[bytes, dt.datetime]:
    with open("tests/samples/realtime_equities.html", "rb") as file:
        return file.read(), dt.datetime(2021, 7, 4)


@fixture
def realtime_indices() -> Tuple[bytes, dt.datetime]:
    with open("tests/samples/realitme_indices.html", "rb") as file:
        return file.read(), dt.datetime(2021, 7, 4)