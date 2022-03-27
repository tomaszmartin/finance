"""This module contains common helper functions."""
import copy
import re
from typing import Any, Optional


def to_snake(original: str) -> str:
    """Changes string to it's 'snake_case' version.

    Example:
        >>> to_snake("This that")
        'this_that'

    Args:
        original: Original string value.

    Returns:
        Snake case version of the string
    """
    res = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", original)
    res = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", res)
    res = re.sub(r"\s", "_", res)
    return res.lower()


def to_float(
    value: str, thusands_sep: str = ",", decimal_sep: str = "."
) -> Optional[float]:
    """Casts string to float or returns None if not possible.
    It handles the ambiguity of ',' used as thousands separator.

    Args:
        value: Value that can possibly convert a float.
        thusands_sep: Separator used for separating thousounds,
         for example in 100,000 the value ',' is used. It can
         differ for different regions.
        decimal_sep: Separator used for dividing decimal part
         in float values, for example in 9.99 the value '.' is
         used.

    Returns:
        float or null
    """
    if value in ["-", "", "---", "x"]:
        return None
    value = value.replace(thusands_sep, "")
    value = value.replace(decimal_sep, ".")
    return float(value)


def rename(the_dict: dict[str, Any], rename_keys: dict[str, str]) -> dict[str, Any]:
    """Rename keys in a dict.
    It does two things:
    - Creates a 'new_key' with value of 'old_key'.
    - Removes the 'old_key'.
    It should not modify existsting dict.

    Examples:
        >>> rename({"a": 0}, {"a": "b"})
        {'b': 0}

    Args:
        the_dict: Dict to be changed.
        rename_keys: Dict with key mappings.

    Returns:
        New dict with renamed keys.
    """
    new_dict = copy.deepcopy(the_dict)
    for old_key, new_key in rename_keys.items():
        if old_key in new_dict:
            new_dict[new_key] = new_dict.pop(old_key)
    return new_dict


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
