import datetime as dt
from typing import Tuple

from pytest import fixture


@fixture
def sample_equities() -> Tuple[bytes, dt.datetime]:
    with open("tests/samples/equities.html", "rb") as file:
        return file.read(), dt.datetime(2021, 1, 4)