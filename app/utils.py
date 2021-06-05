import datetime as dt
import re
import typing


def to_snake(original: str) -> str:
    res = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", original)
    res = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", res).lower()
    res = re.sub(r"\s", "_", res)
    return res
