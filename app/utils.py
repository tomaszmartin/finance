import datetime as dt
import re
import typing


def to_snake(original: str) -> str:
    res = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", original)
    res = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", res).lower()
    res = re.sub(r"\s", "_", res)
    return res


def days_between(
    since: dt.date,
    until: dt.date,
    ascending: bool = True,
) -> typing.Iterator[dt.date]:
    if until < since:
        raise ValueError("Until date must be greater or equal to since.")
    step = 1
    result = []
    for day in range(0, int((until - since).days + 1), step):
        if ascending:
            curr = since + dt.timedelta(days=day)
        else:
            curr = until - dt.timedelta(days=day)
        result.append(curr)
    return result
