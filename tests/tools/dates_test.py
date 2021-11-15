import datetime as dt

from app.tools import dates


def test_polish_holidays():
    all_dates = set(dt.date(2021, 1, 1) + dt.timedelta(days=i) for i in range(365))
    holidays = {
        dt.date(2021, 1, 1),
        dt.date(2021, 1, 6),
        dt.date(2021, 4, 4),
        dt.date(2021, 4, 5),
        dt.date(2021, 5, 1),
        dt.date(2021, 5, 3),
        dt.date(2021, 5, 23),
        dt.date(2021, 6, 3),
        dt.date(2021, 8, 15),
        dt.date(2021, 11, 1),
        dt.date(2021, 11, 11),
        dt.date(2021, 12, 25),
        dt.date(2021, 12, 26),
    }
    non_holidays = all_dates.difference(holidays)
    for date in holidays:
        assert dates.is_holiday(date)
    for date in non_holidays:
        assert not dates.is_holiday(date)
