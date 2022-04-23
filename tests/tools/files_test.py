import pytest

from app.tools import files


@pytest.mark.parametrize(
    "ext,bytes_data",
    [
        ("json", b'[{"test": true}, {"test": false}]'),
        ("jsonl", b'{"test": true}\n{"test": false}'),
    ],
)
def test_to_bytes(ext, bytes_data):
    filename = f"test.{ext}"
    data = [{"test": True}, {"test": False}]
    result = files.to_bytes(filename, data)
    assert result == bytes_data


@pytest.mark.parametrize(
    "ext,bytes_data",
    [
        ("json", b'[{"a": true, "b": false}]'),
        ("jsonl", b'{"a": true, "b": false}'),
    ],
)
def test_to_bytes_key_order(ext, bytes_data):
    """Checks whether saving to 'json' or 'jsonl'
    orders keys.
    """
    filename = f"test.{ext}"
    data = [{"b": False, "a": True}]
    result = files.to_bytes(filename, data)
    assert result == bytes_data


def test_to_bytes_bad_format():
    filename = "test.error"
    data = [{"test": True}, {"test": False}]
    with pytest.raises(ValueError):
        files.to_bytes(filename, data)


@pytest.mark.parametrize(
    "ext,bytes_data",
    [
        ("json", b'[{"test": true}, {"test": false}]'),
        ("jsonl", b'{"test": true}\n{"test": false}'),
    ],
)
def test_from_bytes(ext, bytes_data):
    filename = f"test.{ext}"
    result = files.from_bytes(filename, bytes_data)
    assert result == [{"test": True}, {"test": False}]


def test_from_bytes_bad_format():
    filename = "test.error"
    with pytest.raises(ValueError):
        files.from_bytes(filename, b"")


@pytest.mark.parametrize(
    "filename,ext",
    [
        ("test.json", "json"),
        ("test.jsonl", "jsonl"),
        ("xyz/abc/test.csv", "csv"),
        ("/test.tar.gz", "gz"),
    ],
)
def test_extracting_extension(filename, ext):
    assert files.get_extension(filename) == ext
