"""Functions for creating consistent naming conventions for Data Lake."""
from functools import partial
import logging


def generate_file(
    layer: str = "",
    process: str = "",
    dataset: str = "",
    extension: str = "",
    prefix: str = "",
    with_timestamp: bool = False,
) -> str:
    """Returns a consistent name temaplate for raw layer
    of DataLake. This is where files should be stored
    unchanged (in the sam form as return from the API).

    Args:
        process: for what process the data is downloaded
        dataset: dataset for which the file is downloaded
        extension: file extension
        prefix: file prefix
        with_timestamp: whether the file should be unique
            for datetime or just date.
    """
    if (layer == "") or (process == "") or (dataset == "") or (extension == ""):
        raise ValueError("All arguments need to be filled!")
    base = "{{ execution_date.year }}"
    file = "{{ ds_nodash }}"
    if with_timestamp:
        file = "{{ ts_nodash }}"
    if prefix:
        file = prefix + "_" + file
    result = f"{layer}/{process}/{dataset}/{base}/{file}.{extension}"
    return result


raw = partial(generate_file, layer="raw", with_timestamp=True)
master = partial(generate_file, layer="master")
