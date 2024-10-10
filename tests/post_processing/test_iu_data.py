import pandas as pd
import pytest
from endgame_postprocessing.post_processing.iu_data import IUData, InvalidIUDataFile


def test_iu_data_get_priority_population_iu_missing_raises_exception():
    with pytest.raises(Exception):
        IUData(pd.DataFrame({"IU_CODE": []})).get_priority_population_for_IU("AAA00001")


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
