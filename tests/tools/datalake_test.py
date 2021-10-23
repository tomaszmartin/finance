import pytest

from app.tools import datalake


def test_raw_file_structure():
    assert (
        datalake.raw("process", "dataset", "ext")
        == "raw/process/dataset/{{ execution_date.year }}/{{ ds_nodash }}.ext"
    )


def test_master_file_structure():
    assert (
        datalake.master("process", "dataset", "ext")
        == "master/process/dataset/{{ execution_date.year }}/{{ ds_nodash }}.ext"
    )


@pytest.mark.parametrize("param", ["", None])
def test_generating_file_empty_args(param):
    with pytest.raises(ValueError):
        datalake.generate_file(layer=param)
    with pytest.raises(ValueError):
        datalake.generate_file(process=param)
    with pytest.raises(ValueError):
        datalake.generate_file(dataset=param)
    with pytest.raises(ValueError):
        datalake.generate_file(extension=param)


def test_generating_file_default():
    with pytest.raises(ValueError):
        datalake.generate_file()
