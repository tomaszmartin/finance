import datetime as dt
from typing import Tuple

from pytest import fixture


@fixture
def sample_indices() -> Tuple[bytes, dt.datetime]:
    with open("tests/data/indices_20210104.html", "rb") as file:
        return file.read(), dt.datetime(2021, 1, 4)