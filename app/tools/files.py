import json
from functools import partial
from typing import Any, Callable
import os


def to_bytes(filename: str, data: list[dict[str, Any]]) -> bytes:
    """Converts a list of dicts to a bytes in a proper file
    format. It chooses the file format based on filename.

    Args:
        filename: name or path to the file. File format is picked based on
            extension provided in file name.
        data: data to be converted.

    Raises:
        ValueError: when the file format is not handled

    Returns:
        data converted to bytes in proper format.
    """
    ext = get_extension(filename=filename)
    generators = {"json": _json_bytes, "jsonl": _jsonl_bytes}
    if ext not in generators.keys():
        raise ValueError(f"Unhandled extension '{ext}'.")
    return generators[ext](data)


def from_bytes(filename: str, data: bytes) -> list[dict[str, Any]]:
    """Converts bytes to a list of dictonaries according
    to the format in file name.

    Args:
        filename: name or path to the file. File format is picked based on
            extension provided in file name.
        data: data to be converted.

    Raises:
        ValueError: when the file format is not handled

    Returns:
        list of dicts
    """
    ext = get_extension(filename=filename)
    handlers: dict[str, Any] = {"json": json.loads, "jsonl": _from_jsonl}
    if ext not in handlers.keys():
        raise ValueError(f"Unhandled extension '{ext}'.")
    return handlers[ext](data)


def get_extension(filename: str) -> str:
    """Extracts extension from file name.

    Args:
        filename: name or path to the file.

    Returns:
        extension.
    """
    return os.path.splitext(filename)[1][1:]


def _jsonl_bytes(data: list[dict[str, Any]]) -> bytes:
    return "\n".join(
        map(partial(json.dumps, sort_keys=True, default=str), data)
    ).encode("utf-8")


def _json_bytes(data: list[dict[str, Any]]) -> bytes:
    return json.dumps(data, sort_keys=True, default=str).encode("utf-8")


def _from_jsonl(data: bytes):
    encoded = data.decode("utf8")
    result = []
    for line in encoded.splitlines():
        row = json.loads(line)
        result.append(row)
    return result