from app.dags import stocks


def test_extraction(sample_indices):
    result, datetime = sample_indices
    data = stocks.get_stocks("indices", datetime)
    assert data is not None
    assert len(data) == 91408
    assert len(data) == len(result)
