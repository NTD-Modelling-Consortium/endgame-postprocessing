import warnings
import pandas as pd
import pandas.testing as pdt
import pytest
from endgame_postprocessing.post_processing.iu_data import (
    IUData,
    IUSelectionCriteria,
    InvalidIUDataFile,
    insert_missing_ius,
    preprocess_iu_meta_data,
)

from endgame_postprocessing.post_processing.disease import Disease


def test_insert_missing_ius():
    input_data = pd.DataFrame(
        {"IU_CODE": ["AAA00001"], "Priority_Population_LF": [12345]}
    )
    required_ius = ["AAA00001", "AAA00002"]
    with warnings.catch_warnings(record=True) as w:
        result = insert_missing_ius(input_data, required_ius)

    pdt.assert_frame_equal(
        result,
        pd.DataFrame(
            {
                "IU_CODE": ["AAA00001", "AAA00002"],
                "Priority_Population_LF": [12345.0, 10000.0],
            }
        ),
    )

    assert [str(warning.message) for warning in w] == [
        """1 were missing from the meta data file:     IU_CODE
1  AAA00002"""
    ]


def test_insert_missing_ius_leaves_non_population_columns_as_was():
    input_data = pd.DataFrame(
        {
            "IU_CODE": ["AAA00001"],
            "Priority_Population_LF": [12345],
            "Endemicity_LF": [pd.NA],
        }
    )
    required_ius = ["AAA00001", "AAA00002"]
    with warnings.catch_warnings(record=True) as w:
        result = insert_missing_ius(input_data, required_ius)

    pdt.assert_frame_equal(
        result,
        pd.DataFrame(
            {
                "IU_CODE": ["AAA00001", "AAA00002"],
                "Priority_Population_LF": [12345.0, 10000.0],
                "Endemicity_LF": [pd.NA, pd.NA],
            }
        ),
    )


def test_insert_missing_ius_no_overlap():
    input_data = pd.DataFrame(
        {
            "IU_CODE": ["AAA00001", "AAA00003"],
            "Priority_Population_LF": [12345] * 2,
        }
    )
    required_ius = ["AAA00002", "AAA00004"]
    with warnings.catch_warnings(record=True) as w:
        result = insert_missing_ius(input_data, required_ius)

    pdt.assert_frame_equal(
        result,
        pd.DataFrame(
            {
                "IU_CODE": ["AAA00001", "AAA00002", "AAA00003", "AAA00004"],
                "Priority_Population_LF": [12345.0, 10000.0] * 2,
            }
        ),
    )


def test_insert_missing_ius_duplicate_ius():
    input_data = pd.DataFrame(
        {
            "IU_CODE": ["AAA00001"],
            "Priority_Population_LF": [12345],
        }
    )
    required_ius = ["AAA00002", "AAA00002"]
    with warnings.catch_warnings(record=True) as w:
        result = insert_missing_ius(input_data, required_ius)

    pdt.assert_frame_equal(
        result,
        pd.DataFrame(
            {
                "IU_CODE": ["AAA00001", "AAA00002"],
                "Priority_Population_LF": [12345.0, 10000.0],
            }
        ),
    )


# def test_iu_data_get_priority_population_iu_missing_raises_exception():
#     with pytest.raises(Exception):
#         IUData(
#             pd.DataFrame({"IU_CODE": [], "Priority_Population_LF": []}),
#             disease=Disease.LF,
#             iu_selection_criteria=IUSelectionCriteria.ALL_IUS,
#         ).get_priority_population_for_IU("AAA00001")


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


def test_preprocess_iu_meta_data_contains_duplicate_and_valid_id():
    preprocessed_input = preprocess_iu_meta_data(
        pd.DataFrame(
            {
                "ADMIN0ISO3": ["AAA"] * 2,
                "Priority_Population_LF": [100] * 2,
                "IU_CODE": ["AAA0000000001"] * 2,
                "IU_ID": [1] * 2,
            }
        )
    )

    pdt.assert_frame_equal(
        preprocessed_input,
        pd.DataFrame(
            {
                "ADMIN0ISO3": ["AAA"],
                "Priority_Population_LF": [100],
                "IU_CODE": ["AAA00001"],
                "IU_ID": [1],
            }
        ),
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
