import pandas as pd
import pytest
from endgame_postprocessing.post_processing.iu_data import IUData, InvalidIUDataFile


def test_iu_data_get_priority_population_iu_missing_returns_10000():
    # with pytest.raises(Exception):
    assert (
        IUData(pd.DataFrame({"IU_CODE": []})).get_priority_population_for_IU("AAA00001")
        == 10000
    )


def test_iu_data_get_priority_population_invalid_iu_raises_exception():
    with pytest.raises(Exception):
        IUData(pd.DataFrame({"IU_CODE": []})).get_priority_population_for_IU("AAA0001")


def test_iu_data_get_priority_population_iu_in():
    assert (
        IUData(
            pd.DataFrame({"IU_CODE": ["AAA00001"], "population": [10]})
        ).get_priority_population_for_IU("AAA00001")
        == 10
    )


def test_duplicate_iu_raises_exception():
    with pytest.raises(InvalidIUDataFile):
        IUData(
            pd.DataFrame({"IU_CODE": ["AAA00001", "AAA00001"], "population": [10, 20]})
        )


def test_iu_data_get_ius_in_country_one_iu_one_country():
    assert (
        IUData(
            pd.DataFrame({"ADMIN0ISO3": ["AAA"], "IU_CODE": ["AAA00001"]})
        ).get_total_ius_in_country("AAA")
        == 1
    )


def test_iu_data_get_ius_in_country_many_iu_one_country():
    assert (
        IUData(
            pd.DataFrame(
                {
                    "ADMIN0ISO3": ["AAA"] * 3,
                    "IU_CODE": ["AAA00001", "AAA00002", "AAA00003"],
                }
            )
        ).get_total_ius_in_country("AAA")
        == 3
    )


def test_iu_data_get_ius_in_country_many_iu_many_country():
    assert (
        IUData(
            pd.DataFrame(
                {
                    "ADMIN0ISO3": ["AAA"] * 3 + ["BBB"],
                    "IU_CODE": ["AAA00001", "AAA00002", "AAA00003", "BBB00001"],
                }
            )
        ).get_total_ius_in_country("AAA")
        == 3
    )
