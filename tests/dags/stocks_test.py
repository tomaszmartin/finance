import datetime as dt
from app.dags import stocks


def test_extraction():
    data = stocks.get_stocks("equities", dt.datetime(2020, 1, 1))
    assert data is not None
    assert len(data) == 61060
