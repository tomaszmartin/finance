from app import utils


def test_snake_case_converision():
    assert utils.to_snake("Test 1") == "test_1"


def test_to_float():
    assert utils.to_float("1") == 1.0
    assert utils.to_float("1.23") == 1.23
    assert utils.to_float("1,000.23") == 1000.23
    assert utils.to_float("-") == None


def test_dict_rename():
    the_dict = {"cola": "val"}
    rename = {"cola": "colb", "colx": "colz"}
    assert utils.rename(the_dict, rename) == {"colb": "val"}


def test_key_drop():
    the_dict = {"cola": "val", "colb": "val"}
    assert utils.drop(the_dict, ["cola"]) == {"colb": "val"}


def test_key_drop_missing_cols():
    the_dict = {"cola": "val", "colb": "val"}
    assert utils.drop(the_dict, ["colc"]) == the_dict
