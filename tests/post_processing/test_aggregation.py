import numpy as np
import pandas as pd
import pandas.testing as pdt
from pyfakefs.fake_filesystem import FakeFilesystem
import pytest
from endgame_postprocessing.post_processing import aggregation


def test_aggregate_post_processed_files_empty_directory_empty_dataframe(fs):
    empty_df = aggregation.aggregate_post_processed_files("empty_directory")
    assert empty_df.empty
    pdt.assert_frame_equal(
        aggregation.aggregate_post_processed_files("empty_directory"), pd.DataFrame([], columns=[])
    )


def test_aggregate_post_processed_files_dir_containing_two_csvs_concatenated(
    fs: FakeFilesystem,
):
    fs.create_file(
        "dir/a.csv", contents=pd.DataFrame({"A": [1], "B": [2]}).to_csv(index=False)
    )
    fs.create_file(
        "dir/b.csv", contents=pd.DataFrame({"A": [3], "B": [4]}).to_csv(index=False)
    )
    expected_combined_data = pd.DataFrame({"A": [1, 3], "B": [2, 4]})
    actual = aggregation.aggregate_post_processed_files("dir")
    pdt.assert_frame_equal(
        # Have to manually re-type as the current loading sets these as ints
        actual.astype(dtype={"A": int, "B": int}),
        expected_combined_data,
    )


def test_aggregate_post_processed_files_dir_containing_two_csvs_with_mismatched_columns_concatenated(
    fs: FakeFilesystem,
):
    fs.create_file(
        "dir/a.csv", contents=pd.DataFrame({"A": [1], "B": [2]}).to_csv(index=False)
    )
    fs.create_file(
        "dir/b.csv", contents=pd.DataFrame({"C": [3], "D": [4]}).to_csv(index=False)
    )
    expected_combined_data = pd.DataFrame({"A": [1, 3], "B": [2, 4]})
    actual = aggregation.aggregate_post_processed_files("dir")
    pdt.assert_frame_equal(
        # Have to manually re-type as the current loading sets these as ints
        actual.astype(dtype={"A": int, "B": int}),
        expected_combined_data,
    )
    # Filter should include only a file


def test_aggregate_post_processed_files_dir_containing_nested_csvs_concatenated(
    fs: FakeFilesystem,
):
    fs.create_file(
        "dir/a.csv", contents=pd.DataFrame({"A": [1], "B": [2]}).to_csv(index=False)
    )
    fs.create_file(
        "dir/nested/b.csv",
        contents=pd.DataFrame({"A": [3], "B": [4]}).to_csv(index=False),
    )
    expected_combined_data = pd.DataFrame({"A": [1, 3], "B": [2, 4]})
    actual = aggregation.aggregate_post_processed_files("dir")
    pdt.assert_frame_equal(
        # Have to manually re-type as the current loading sets these as ints
        actual.astype(dtype={"A": int, "B": int}),
        expected_combined_data,
    )


def test_aggregate_post_processed_files_dir_containing_two_csvs_filter_set(
    fs: FakeFilesystem,
):
    fs.create_file(
        "dir/a.csv", contents=pd.DataFrame({"A": [1], "B": [2]}).to_csv(index=False)
    )
    fs.create_file(
        "dir/b.csv", contents=pd.DataFrame({"A": [3], "B": [4]}).to_csv(index=False)
    )
    expected_combined_data = pd.DataFrame({"A": [1], "B": [2]})
    actual = aggregation.aggregate_post_processed_files("dir", specific_files="a.csv")
    pdt.assert_frame_equal(
        # Have to manually re-type as the current loading sets these as ints
        actual.astype(dtype={"A": int, "B": int}),
        expected_combined_data,
    )


def test_iu_lvl_aggregate_mean_replaced_with_nan():
    df_with_mean = pd.DataFrame({"mean": ["", 1.0]})
    iu_aggregate = aggregation.iu_lvl_aggregate(
        df_with_mean, typing_map={"mean": float}
    )
    pdt.assert_frame_equal(iu_aggregate, pd.DataFrame({"mean": [np.nan, 1.0]}))


def test_iu_lvl_aggregate_non_mean_replaced_with_none():
    df_with_mean_and_other = pd.DataFrame({"other": ["", 1.0], "mean": [1.0, 2.0]})
    iu_aggregate = aggregation.iu_lvl_aggregate(
        df_with_mean_and_other, typing_map={"mean": float, "other": float}
    )
    pdt.assert_frame_equal(
        iu_aggregate, pd.DataFrame({"other": [None, 1.0], "mean": [1.0, 2.0]})
    )


def test_iu_lvl_aggregate_incorrectly_typed_mean_raises_type_error():
    iu_data_with_type_error = pd.DataFrame({"mean": ["wrong type"]})
    with pytest.raises(ValueError):
        aggregation.iu_lvl_aggregate(
            iu_data_with_type_error, typing_map={"mean": float}
        )

# column containing an NA and a NaN


def test_country_lvl_aggregate_aggregate_by_country_general_measures():
    iu_data = pd.DataFrame(
        {
            "country": ["C1"] * 2 + ["C2"] * 2,
            "measure": ["M1"] * 4,
            "mean": [0.2, 0.4, 0.6, 0.8],
        }
    )
    aggregate_data = aggregation.country_lvl_aggregate(
        iu_data,
        measure_column_name="measure",
        general_summary_measure_names=["M1"],
        general_groupby_cols=["country", "measure"],
        threshold_cols_rename={},
        threshold_groupby_cols=[],
        threshold_summary_cols=[],
    )
    pdt.assert_frame_equal(
        aggregate_data,
        pd.DataFrame(
            {
                "country": ["C1", "C2"], "measure": ["M1"] * 2,
                "mean": [0.3, 0.7], "2.5_percentile": [0.205, 0.605], 
                "5_percentile": [0.21, 0.61], "10_percentile": [0.22, 0.62],
                "25_percentile": [0.25, 0.65], "50_percentile": [0.3, 0.7],
                "75_percentile": [0.35, 0.75], "90_percentile": [0.38, 0.78],
                "95_percentile": [0.39, 0.79], "97.5_percentile": [0.395, 0.795],
                "standard_deviation": [np.std([0.2, 0.4], ddof=1),np.std([0.6, 0.8], ddof=1)],
                "median": [np.median([0.2, 0.4]), np.median([0.6, 0.8])]
             }
        ),
    )

def test_country_lvl_aggregate_aggregate_by_country_fail_threshold_measures():
    iu_data = pd.DataFrame(
        {
            "country": ["C1"] * 2 + ["C2"] * 2,
            "measure": ["M1"] * 4,
            "mean": [0.2, 0.4, 0.6, 0.8],
        }
    )
    with pytest.raises(ValueError):
        aggregation.country_lvl_aggregate(
            iu_data,
            measure_column_name="measure",
            general_summary_measure_names=["M1"],
            general_groupby_cols=["country", "measure"],
            threshold_cols_rename={},
            threshold_groupby_cols=[],
            threshold_summary_cols=["country"],
        )

# Test rename the measure column for summarize (I think there is a bug here)
