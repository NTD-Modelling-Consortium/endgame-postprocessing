import math
import warnings
import pandas as pd
import pandas.testing as pdt
import pytest
from endgame_postprocessing.post_processing.iu_data import (
    IUData,
    IUSelectionCriteria,
    InvalidIUDataFile,
)

from endgame_postprocessing.post_processing.disease import Disease


def test_iu_data_get_priority_population_iu_missing_raises_exception():
    with pytest.raises(Exception):
        IUData(
            pd.DataFrame({"IU_CODE": [], "Priority_Population_LF": []}),
            disease=Disease.LF,
            iu_selection_criteria=IUSelectionCriteria.ALL_IUS,
        ).get_priority_population_for_IU("AAA00001")


def test_iu_data_without_valid_priority_population_column_raises_exception():
    with pytest.raises(Exception) as e:
        IUData(
            pd.DataFrame({"IU_CODE": [], "Priority_Population_InvalidDisease": []}),
            disease=Disease.LF,
            iu_selection_criteria=IUSelectionCriteria.ALL_IUS,
        )
    assert e.match(
        "No priority population found for disease LF, expected Priority_Population_LF"
    )


def test_iu_data_get_priority_population_invalid_iu_raises_exception():
    with pytest.raises(Exception):
        IUData(pd.DataFrame({"IU_CODE": []})).get_priority_population_for_IU("AAA0001")


def test_iu_data_get_priority_population_iu_in():
    assert (
        IUData(
            pd.DataFrame({"IU_CODE": ["AAA00001"], "Priority_Population_LF": [10]}),
            disease=Disease.LF,
            iu_selection_criteria=IUSelectionCriteria.ALL_IUS,
        ).get_priority_population_for_IU("AAA00001")
        == 10
    )


def test_iu_data_get_priority_population_iu_in_from_oncho():
    assert (
        IUData(
            pd.DataFrame(
                {
                    "IU_CODE": ["AAA00001"],
                    "Priority_Population_LF": [10],
                    "Priority_Population_Oncho": [20],
                }
            ),
            disease=Disease.ONCHO,
            iu_selection_criteria=IUSelectionCriteria.ALL_IUS,
        ).get_priority_population_for_IU("AAA00001")
        == 20
    )


def test_duplicate_iu_raises_exception():
    with pytest.raises(InvalidIUDataFile):
        IUData(
            pd.DataFrame(
                {
                    "IU_CODE": ["AAA00001", "AAA00001"],
                    "Priority_Population_LF": [10, 20],
                }
            ),
            disease=Disease.LF,
            iu_selection_criteria=IUSelectionCriteria.ALL_IUS,
        )


def test_iu_data_get_ius_in_country_one_iu_one_country():
    assert (
        IUData(
            pd.DataFrame(
                {
                    "ADMIN0ISO3": ["AAA"],
                    "IU_CODE": ["AAA00001"],
                    "Priority_Population_LF": [10],
                }
            ),
            disease=Disease.LF,
            iu_selection_criteria=IUSelectionCriteria.ALL_IUS,
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
                    "Priority_Population_LF": [10] * 3,
                }
            ),
            disease=Disease.LF,
            iu_selection_criteria=IUSelectionCriteria.ALL_IUS,
        ).get_total_ius_in_country("AAA")
        == 3
    )


def test_iu_data_get_ius_in_country_only_modelled():
    assert (
        IUData(
            pd.DataFrame(
                {
                    "ADMIN0ISO3": ["AAA"] * 3,
                    "IU_CODE": ["AAA00001", "AAA00002", "AAA00003"],
                    "Priority_Population_LF": [10] * 3,
                    "Modelled_LF": [True, False, False],
                }
            ),
            disease=Disease.LF,
            iu_selection_criteria=IUSelectionCriteria.MODELLED_IUS,
        ).get_total_ius_in_country("AAA")
        == 1
    )


def test_iu_data_get_ius_in_country_only_endemic_lf():
    assert (
        IUData(
            pd.DataFrame(
                {
                    "ADMIN0ISO3": ["AAA"] * 3,
                    "IU_CODE": ["AAA00001", "AAA00002", "AAA00003"],
                    "Priority_Population_LF": [10] * 3,
                    "Encemicity_LF": [
                        "Endemic (MDA not delivered)",
                        "Non-endemic",
                        "Non-endemic",
                    ],
                }
            ),
            disease=Disease.LF,
            iu_selection_criteria=IUSelectionCriteria.ENDEMIC_IUS,
        ).get_total_ius_in_country("AAA")
        == 1
    )


def test_iu_data_get_ius_in_country_many_iu_many_country():
    assert (
        IUData(
            pd.DataFrame(
                {
                    "ADMIN0ISO3": ["AAA"] * 3 + ["BBB"],
                    "IU_CODE": ["AAA00001", "AAA00002", "AAA00003", "BBB00001"],
                    "Priority_Population_LF": [10] * 4,
                }
            ),
            disease=Disease.LF,
            iu_selection_criteria=IUSelectionCriteria.ALL_IUS,
        ).get_total_ius_in_country("AAA")
        == 3
    )


def test_iu_data_get_ius_in_country_many_iu_many_country_include_only_modelled():
    assert (
        IUData(
            pd.DataFrame(
                {
                    "ADMIN0ISO3": ["AAA"] * 3 + ["BBB"],
                    "IU_CODE": ["AAA00001", "AAA00002", "AAA00003", "BBB00001"],
                    "Priority_Population_LF": [10] * 4,
                    "Modelled_LF": [True, False, False, True],
                }
            ),
            disease=Disease.LF,
            iu_selection_criteria=IUSelectionCriteria.MODELLED_IUS,
        ).get_total_ius_in_country("AAA")
        == 1
    )


def test_get_population_for_country():
    assert (
        IUData(
            pd.DataFrame(
                {
                    "ADMIN0ISO3": ["AAA"] * 3 + ["BBB"],
                    "Priority_Population_LF": [100, 200, 300, 400],
                    "IU_CODE": ["AAA00001", "AAA00002", "AAA00003", "BBB00001"],
                }
            ),
            disease=Disease.LF,
            iu_selection_criteria=IUSelectionCriteria.ALL_IUS,
        ).get_priority_population_for_country("AAA")
        == 600
    )


def test_get_population_for_country_modelled_only():
    assert (
        IUData(
            pd.DataFrame(
                {
                    "ADMIN0ISO3": ["AAA"] * 3 + ["BBB"],
                    "Priority_Population_LF": [100, 200, 300, 400],
                    "IU_CODE": ["AAA00001", "AAA00002", "AAA00003", "BBB00001"],
                    "Modelled_LF": [True, False, False, True],
                }
            ),
            disease=Disease.LF,
            iu_selection_criteria=IUSelectionCriteria.MODELLED_IUS,
        ).get_priority_population_for_country("AAA")
        == 100
    )


def test_get_africa_population():
    assert (
        IUData(
            pd.DataFrame(
                {
                    "ADMIN0ISO3": ["AAA"] * 3 + ["BBB"],
                    "Priority_Population_LF": [100, 200, 300, 400],
                    "IU_CODE": ["AAA00001", "AAA00002", "AAA00003", "BBB00001"],
                }
            ),
            disease=Disease.LF,
            iu_selection_criteria=IUSelectionCriteria.ALL_IUS,
        ).get_priority_population_for_africa()
        == 1000
    )


def test_get_africa_population_modelled_ius_only():
    assert (
        IUData(
            pd.DataFrame(
                {
                    "ADMIN0ISO3": ["AAA"] * 3 + ["BBB"],
                    "Priority_Population_LF": [100, 200, 300, 400],
                    "IU_CODE": ["AAA00001", "AAA00002", "AAA00003", "BBB00001"],
                    "Modelled_LF": [True, False, False, True],
                }
            ),
            disease=Disease.LF,
            iu_selection_criteria=IUSelectionCriteria.MODELLED_IUS,
        ).get_priority_population_for_africa()
        == 500
    )


def test_get_africa_population_endemic_ius_only():
    assert (
        IUData(
            pd.DataFrame(
                {
                    "ADMIN0ISO3": ["AAA"] * 3 + ["BBB"],
                    "Priority_Population_Oncho": [100, 200, 300, 400],
                    "IU_CODE": ["AAA00001", "AAA00002", "AAA00003", "BBB00001"],
                    "Encemicity_Oncho": [
                        "Endemic (MDA not delivered)",
                        "Non-endemic",
                        "Non-endemic",
                        "Endemic (MDA not delivered)",
                    ],
                }
            ),
            disease=Disease.ONCHO,
            iu_selection_criteria=IUSelectionCriteria.ENDEMIC_IUS,
        ).get_priority_population_for_africa()
        == 500
    )


def test_simulated_ius_includes_simulated_iu():
    assert (
        IUData(
            pd.DataFrame(
                {
                    "ADMIN0ISO3": ["AAA"] * 3 + ["BBB"],
                    "Priority_Population_Oncho": [100, 200, 300, 400],
                    "IU_CODE": ["AAA00001", "AAA00002", "AAA00003", "BBB00001"],
                }
            ),
            disease=Disease.ONCHO,
            iu_selection_criteria=IUSelectionCriteria.SIMULATED_IUS,
            simulated_IUs=["AAA00001", "BBB00001"],
        ).get_priority_population_for_africa()
        == 500
    )
