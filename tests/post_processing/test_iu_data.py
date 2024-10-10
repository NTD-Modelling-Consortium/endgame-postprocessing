import pytest
from endgame_postprocessing.post_processing.iu_data import IUData


def test_iu_data_get_priority_population_iu_missing():
    assert IUData({}).get_priority_population_for_IU("AAA00001") == 10000


def test_iu_data_get_priority_population_invalid_iu_raises_exception():
    with pytest.raises(Exception):
        IUData({}).get_priority_population_for_IU("AAA0001")


def test_iu_data_get_priority_population_iu_in():
    assert IUData({"AAA00001": 10}).get_priority_population_for_IU("AAA00001") == 10
