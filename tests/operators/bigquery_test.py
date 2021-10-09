from app.operators.bigquery import BigQueryValidateDataOperator


def test_verifying_rows_empty():
    row = {}
    assert not BigQueryValidateDataOperator.verify(row)


def test_verifying_rows_false():
    row = {"test": False}
    assert not BigQueryValidateDataOperator.verify(row)


def test_verifying_rows_any_false():
    row = {"ok": True, "test": False}
    assert not BigQueryValidateDataOperator.verify(row)


def test_verifying_ok():
    row = {"ok": True, "test": True}
    assert BigQueryValidateDataOperator.verify(row)
