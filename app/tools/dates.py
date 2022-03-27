"""Helper module for working with dates."""
import datetime as dt

import holidays


def is_holiday(execution_date: dt.date) -> bool:
    """Verifies whether given date
    is a holiday in Poland.

    Args:
        execution_date: date to be verified

    Returns:
        True if date is a holiday, False otherwise
    """
    pl_holidays = holidays.Poland()
    return execution_date in pl_holidays


def is_workday(execution_date: dt.date) -> bool:
    """Verifies whether given date
    is a workday in Poland.

    Args:
        execution_date: date to be verified

    Returns:
        True if give date is a working day
    """
    if execution_date.weekday() in (5, 6):
        return False
    return not is_holiday(execution_date)
