import datetime as dt
from app import utils


def test_snake_case_converision():
    assert utils.to_snake("Test 1") == "test_1"
