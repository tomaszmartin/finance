import datetime as dt
from app import utils


def test_snake_case_converision():
    assert utils.to_snake("Test 1") == "test_1"


def test_dates_between():
    since = dt.date(2020, 1, 1)
    until = dt.date(2020, 1, 7)
    assert utils.days_between(since, until) == [
        dt.date(2020, 1, 1),
        dt.date(2020, 1, 2),
        dt.date(2020, 1, 3),
        dt.date(2020, 1, 4),
        dt.date(2020, 1, 5),
        dt.date(2020, 1, 6),
        dt.date(2020, 1, 7),
    ]
