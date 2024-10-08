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
        aggregation.aggregate_post_processed_files("empty_directory"),
        pd.DataFrame([], columns=[]),
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


def test_aggregate_post_processed_files_dir_containing_two_csvs_w_mismatched_columns_concatenated(
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


def test_single_country_lvl_aggregate_aggregate_by_country_general_measures():
    iu_data = pd.DataFrame(
        {
            "scenario": ["scenario_1"],
            "country_code": ["C1"],
            "measure": ["M1"],
            "year_id": [2010],
            "draw_0": [0.2],
            "draw_1": [0.4],
        }
    )
    aggregate_data = aggregation.single_country_aggregate(iu_data)
    pdt.assert_frame_equal(
        aggregate_data,
        pd.DataFrame(
            {
                "scenario": ["scenario_1"],
                "country_code": ["C1"],
                "measure": ["M1"],
                "year_id": [2010],
                "mean": [0.3],
                "2.5_percentile": [0.205],
                "5_percentile": [0.21],
                "10_percentile": [0.22],
                "25_percentile": [0.25],
                "50_percentile": [0.3],
                "75_percentile": [0.35],
                "90_percentile": [0.38],
                "95_percentile": [0.39],
                "97.5_percentile": [0.395],
                "standard_deviation": [
                    np.std([0.2, 0.4]),
                ],
                "median": [np.median([0.2, 0.4])],
            }
        ),
        check_dtype=False,  # TODO: why is type of year lost
    )


def test_country_lvl_aggregate_raises_error_if_provided_groupby_without_any_measures_for_threshold():  # noqa: E501
    iu_data = pd.DataFrame(
        {
            "country": ["C1"] * 2,
            "measure": ["M1"] * 2,
            "mean": [0.2, 0.4],
        }
    )
    with pytest.raises(ValueError):
        aggregation.country_lvl_aggregate(
            iu_data,
            threshold_cols_rename={},
            threshold_groupby_cols=["random_group_by"],
            threshold_summary_measure_names=[],
            denominator_to_use=1,
        )


# Test rename the measure column for summarize (I think there is a bug here)
def test_country_lvl_aggregate_aggregate_by_country_rename():
    iu_data = pd.DataFrame(
        {
            "country": ["C1"] + ["C2"],
            "measure": ["M2"] * 2,
            "mean": [None, 12],
        }
    )
    aggregate_data = aggregation.country_lvl_aggregate(
        iu_data,
        threshold_cols_rename={"M2": "test", "M1": "should not rename"},
        threshold_groupby_cols=["measure"],
        threshold_summary_measure_names=["M2"],
        denominator_to_use=2,
    )
    pdt.assert_frame_equal(
        aggregate_data,
        pd.DataFrame(
            {
                "measure": ["pct_of_test", "count_of_test", "year_of_test"],
                "mean": [0.5, 1.0, 12.0],
            }
        ),
    )


def test_africa_lvl_aggregate_success():
    composite_africa_data = pd.DataFrame(
        {
            "year_id": [2010],
            "scenario": ["scenario_1"],
            "measure": ["M1"],
            "draw_0": [0.2],
            "draw_1": [0.6],
            "draw_2": [0.4],
            "draw_3": [0.8],
        }
    )

    africa_data = aggregation.africa_lvl_aggregate(
        composite_africa_data,
    )
    pdt.assert_frame_equal(
        africa_data,
        pd.DataFrame(
            {
                "scenario": ["scenario_1"],
                "measure": ["M1"],
                "year_id": [2010],
                "mean": [0.5],
                "2.5_percentile": [0.215],
                "5_percentile": [0.23],
                "10_percentile": [0.26],
                "25_percentile": [0.35],
                "50_percentile": [0.5],
                "75_percentile": [0.65],
                "90_percentile": [0.74],
                "95_percentile": [0.77],
                "97.5_percentile": [0.785],
                "standard_deviation": [np.std([0.2, 0.4, 0.6, 0.8])],
                "median": [0.5],
            }
        ),
        check_dtype=False,
    )
