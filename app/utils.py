import re
from typing import Optional


def to_snake(original: str) -> str:
    res = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", original)
    res = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", res).lower()
    res = re.sub(r"\s", "_", res)
    return res


def to_float(value: str) -> Optional[float]:
    if value == "-":
        return None
    value = value.replace(",", "")
    return float(value)


def rename(the_dict, rename_keys):
    for old_key, new_key in rename_keys.items():
        if old_key in the_dict:
            the_dict[new_key] = the_dict.pop(old_key)
    return the_dict


def drop(the_dict, columns):
    for key in columns:
        if key in the_dict:
            the_dict.pop(key)
    return the_dict
