"""Helper module for working with dates."""
import datetime as dt
import holidays


def is_holiday(date: dt.date) -> bool:
    """Verifies whether given date is a polish
    holiday.

    Args:
        date: date to be verified

    Returns:
        True if date is a holiday, False otherwise
    """
    pl_holidays = holidays.Poland()
    return date in pl_holidays
