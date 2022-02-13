import datetime as dt
from functools import partial
from unittest import mock

import pandas as pd
import pytest

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


@pytest.fixture
def mocked_bq_call(request, db_session):
    with mock.patch(
        "app.operators.bigquery.BigQueryValidateDataOperator.get_data_from_bq"
    ) as mocked_get_call:
        db_session.execute("CREATE TABLE test (col BOOL);")
        db_session.execute(
            "INSERT INTO test (col) VALUES (:col)", {"col": request.param}
        )
        mocked_get_call.side_effect = partial(pd.read_sql, con=db_session)
        yield mocked_get_call


@pytest.mark.parametrize("mocked_bq_call", [False], indirect=True)
def test_execute_validates_errors(mocked_bq_call):
    query = "SELECT * FROM test"
    task = BigQueryValidateDataOperator(task_id="query", gcp_conn_id="conn", sql=query)
    with pytest.raises(AssertionError):
        task.execute(context={})


@pytest.mark.parametrize("mocked_bq_call", [True], indirect=True)
def test_execute_validates_pass(mocked_bq_call):
    try:
        query = "SELECT * FROM test"
        task = BigQueryValidateDataOperator(
            task_id="query", gcp_conn_id="conn", sql=query
        )
        task.execute(context={})
    except AssertionError:
        pytest.fail("BigQueryValidateDataOperator should not throw error.")
