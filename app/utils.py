"""Contains common helper functions, used my multiple modules.
"""
import re
from typing import Any, Optional


def to_snake(original: str) -> str:
    """Changes string to it's snake_case version.

    Example:
        to_snake("This that") == "this_that"

    Args:
        original: original string value.

    Returns:
        snake_case version of the string
    """
    res = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", original)
    res = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", res).lower()
    res = re.sub(r"\s", "_", res)
    return res


def to_float(
    value: str, thusands_sep: str = ",", decimal_sep: str = "."
) -> Optional[float]:
    """Casts string to float or returns None if not possible.
    It handles the ambiguity of ',' used as thousands separator.

    Args:
        value: possible float as string

    Returns:
        float or null
    """
    if value == "-":
        return None
    value = value.replace(thusands_sep, "")
    value = value.replace(decimal_sep, ".")
    return float(value)


def rename(the_dict: dict[str, Any], rename_keys: dict[str, str]) -> dict[str, Any]:
    """Rename keys in a dict. It does two things:
    - creates a key with value of old key
    - removes the old key

    Args:
        the_dict: dict to be changed
        rename_keys: dict with key mappings

    Returns:
        new dict
    """
    for old_key, new_key in rename_keys.items():
        if old_key in the_dict:
            the_dict[new_key] = the_dict.pop(old_key)
    return the_dict


def drop(the_dict: dict[str, Any], columns: list[str]) -> dict[str, Any]:
    """Rename keys from a dict, but only if they are present.

    Args:
        the_dict: dict to be changed
        columns: list with keys

    Returns:
        new dict
    """
    for key in columns:
        if key in the_dict:
            the_dict.pop(key)
    return the_dict
