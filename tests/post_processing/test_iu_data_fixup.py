import math
import warnings
import pandas as pd
import pandas.testing as pdt

from endgame_postprocessing.post_processing.iu_data_fixup import (
    insert_missing_ius,
    fixup_iu_meta_data_file,
    remove_non_simulated_ius,
)


def test_insert_missing_ius():
    input_data = pd.DataFrame(
        {
            "IU_CODE": ["AAA00001"],
            "ADMIN0ISO3": ["AAA"],
            "Priority_Population_LF": [12345],
        }
    )
    required_ius = ["AAA00001", "AAA00002"]
    with warnings.catch_warnings(record=True) as w:
        result = insert_missing_ius(input_data, set(required_ius))

    pdt.assert_frame_equal(
        result,
        pd.DataFrame(
            {
                "IU_CODE": ["AAA00001", "AAA00002"],
                "ADMIN0ISO3": ["AAA"] * 2,
                "Priority_Population_LF": [12345.0, 10000.0],
            }
        ),
    )

    assert [str(warning.message) for warning in w] == [
        """1 were missing from the meta data file: ['AAA00002']""",
        "For these IUs a default population of 10000.0 will be used",
    ]


def test_insert_missing_ius_none_missing():
    input_data = pd.DataFrame(
        {
            "IU_CODE": ["AAA00001"],
            "ADMIN0ISO3": ["AAA"],
            "Priority_Population_LF": [12345],
        }
    )
    required_ius = ["AAA00001"]
    with warnings.catch_warnings(record=True) as w:
        result = insert_missing_ius(input_data, set(required_ius))

    pdt.assert_frame_equal(
        result,
        pd.DataFrame(
            {
                "IU_CODE": ["AAA00001"],
                "ADMIN0ISO3": ["AAA"],
                "Priority_Population_LF": [12345],
            }
        ),
    )

    assert len(w) == 0


def test_insert_missing_ius_leaves_non_population_columns_as_was():
    input_data = pd.DataFrame(
        {
            "IU_CODE": ["AAA00001"],
            "ADMIN0ISO3": ["AAA"],
            "Priority_Population_LF": [12345],
            "Endemicity_LF": [pd.NA],
        }
    )
    required_ius = ["AAA00001", "AAA00002"]
    with warnings.catch_warnings(record=True):
        result = insert_missing_ius(input_data, set(required_ius))

    pdt.assert_frame_equal(
        result,
        pd.DataFrame(
            {
                "IU_CODE": ["AAA00001", "AAA00002"],
                "ADMIN0ISO3": ["AAA"] * 2,
                "Priority_Population_LF": [12345.0, 10000.0],
                "Endemicity_LF": [pd.NA, pd.NA],
            }
        ),
    )


def test_insert_missing_ius_no_overlap():
    input_data = pd.DataFrame(
        {
            "IU_CODE": ["AAA00001", "AAA00003"],
            "ADMIN0ISO3": ["AAA"] * 2,
            "Priority_Population_LF": [12345] * 2,
        }
    )
    required_ius = ["AAA00002", "AAA00004"]
    with warnings.catch_warnings(record=True):
        result = insert_missing_ius(input_data, set(required_ius))

    pdt.assert_frame_equal(
        result,
        pd.DataFrame(
            {
                "IU_CODE": ["AAA00001", "AAA00002", "AAA00003", "AAA00004"],
                "ADMIN0ISO3": ["AAA"] * 4,
                "Priority_Population_LF": [12345.0, 10000.0] * 2,
            }
        ),
    )


def test_insert_missing_ius_duplicate_ius():
    input_data = pd.DataFrame(
        {
            "IU_CODE": ["AAA00001"],
            "ADMIN0ISO3": ["AAA"],
            "Priority_Population_LF": [12345],
        }
    )
    required_ius = ["AAA00002", "AAA00002"]
    with warnings.catch_warnings(record=True):
        result = insert_missing_ius(input_data, set(required_ius))

    pdt.assert_frame_equal(
        result,
        pd.DataFrame(
            {
                "IU_CODE": ["AAA00001", "AAA00002"],
                "ADMIN0ISO3": ["AAA"] * 2,
                "Priority_Population_LF": [12345.0, 10000.0],
            }
        ),
    )


def test_preprocess_iu_meta_data_contains_duplicate_and_valid_id():
    preprocessed_input = fixup_iu_meta_data_file(
        pd.DataFrame(
            {
                "ADMIN0ISO3": ["AAA"] * 2,
                "Priority_Population_LF": [100] * 2,
                "IU_CODE": ["AAA0000000001"] * 2,
                "IU_ID": [1] * 2,
            }
        ),
        simulated_IUs=["AAA00001"],
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


def test_preprocess_iu_meta_data_removes_non_simulated_ius():
    preprocessed_input = fixup_iu_meta_data_file(
        pd.DataFrame(
            {
                "ADMIN0ISO3": ["AAA"] * 2,
                "Priority_Population_LF": [100] * 2,
                "IU_CODE": ["AAA0000000001", "AAA0000000002"],
                "IU_ID": [1, 2],
            }
        ),
        simulated_IUs=["AAA00001"],
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


def test_preprocess_iu_meta_data_adds_missing_iu():
    preprocessed_input = fixup_iu_meta_data_file(
        pd.DataFrame(
            {
                "ADMIN0ISO3": ["AAA"],
                "Priority_Population_LF": [100],
                "IU_CODE": ["AAA0000000001"],
                "IU_ID": [1],
            }
        ),
        simulated_IUs=["AAA00001", "AAA00002"],
    )

    pdt.assert_frame_equal(
        preprocessed_input,
        pd.DataFrame(
            {
                "ADMIN0ISO3": ["AAA"] * 2,
                "Priority_Population_LF": [100.0, 10000.0],
                "IU_CODE": ["AAA00001", "AAA00002"],
                "IU_ID": [1.0, math.nan],
            }
        ),
    )


def test_remove_nonsimualted_ius():
    input_data = pd.DataFrame(
        {"IU_CODE": ["AAA123", "AAA234"], "AnotherColumn": [1, 2]}
    )
    result = remove_non_simulated_ius(input_data, simulated_IUs=["AAA123"])
    pdt.assert_frame_equal(
        result, pd.DataFrame({"IU_CODE": ["AAA123"], "AnotherColumn": [1]})
    )
