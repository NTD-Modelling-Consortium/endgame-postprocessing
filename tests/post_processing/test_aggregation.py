import numpy as np
import numpy.testing as npt
import pandas as pd
import pandas.testing as pdt
import pytest
from pyfakefs.fake_filesystem import FakeFilesystem

from endgame_postprocessing.post_processing import aggregation
from endgame_postprocessing.post_processing.constants import PROB_UNDER_THRESHOLD_MEASURE_NAME


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
    fs.create_file("dir/a.csv", contents=pd.DataFrame({"A": [1], "B": [2]}).to_csv(index=False))
    fs.create_file("dir/b.csv", contents=pd.DataFrame({"A": [3], "B": [4]}).to_csv(index=False))
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
    fs.create_file("dir/a.csv", contents=pd.DataFrame({"A": [1], "B": [2]}).to_csv(index=False))
    fs.create_file("dir/b.csv", contents=pd.DataFrame({"C": [3], "D": [4]}).to_csv(index=False))
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
    fs.create_file("dir/a.csv", contents=pd.DataFrame({"A": [1], "B": [2]}).to_csv(index=False))
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
    fs.create_file("dir/a.csv", contents=pd.DataFrame({"A": [1], "B": [2]}).to_csv(index=False))
    fs.create_file("dir/b.csv", contents=pd.DataFrame({"A": [3], "B": [4]}).to_csv(index=False))
    expected_combined_data = pd.DataFrame({"A": [1], "B": [2]})
    actual = aggregation.aggregate_post_processed_files("dir", specific_files="a.csv")
    pdt.assert_frame_equal(
        # Have to manually re-type as the current loading sets these as ints
        actual.astype(dtype={"A": int, "B": int}),
        expected_combined_data,
    )


def test_iu_lvl_aggregate_mean_replaced_with_nan():
    df_with_mean = pd.DataFrame({"mean": ["", 1.0]})
    iu_aggregate = aggregation.iu_lvl_aggregate(df_with_mean, typing_map={"mean": float})
    pdt.assert_frame_equal(iu_aggregate, pd.DataFrame({"mean": [np.nan, 1.0]}))


def test_iu_lvl_aggregate_non_mean_replaced_with_none():
    df_with_mean_and_other = pd.DataFrame({"other": ["", 1.0], "mean": [1.0, 2.0]})
    iu_aggregate = aggregation.iu_lvl_aggregate(
        df_with_mean_and_other, typing_map={"mean": float, "other": float}
    )
    pdt.assert_frame_equal(iu_aggregate, pd.DataFrame({"other": [None, 1.0], "mean": [1.0, 2.0]}))


def test_iu_lvl_aggregate_incorrectly_typed_mean_raises_type_error():
    iu_data_with_type_error = pd.DataFrame({"mean": ["wrong type"]})
    with pytest.raises(ValueError):
        aggregation.iu_lvl_aggregate(iu_data_with_type_error, typing_map={"mean": float})


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
            pct_runs_under_threshold=[0.1],
            denominator_to_use=1,
        )


# Test rename the measure column for summarize (I think there is a bug here)
def test_country_lvl_aggregate_aggregate_by_country_rename():
    iu_data = pd.DataFrame(
        {
            "country": ["C1"] + ["C2"],
            "year_id": [np.nan, np.nan],
            "measure": ["M2"] * 2,
            "mean": [-1, 2012],
        }
    )
    aggregate_data = aggregation.country_lvl_aggregate(
        iu_data,
        threshold_cols_rename={"M2": "test", "M1": "should not rename"},
        threshold_groupby_cols=["measure"],
        threshold_summary_measure_names=["M2"],
        pct_runs_under_threshold=[0.1],
        denominator_to_use=2,
    )
    pdt.assert_frame_equal(
        aggregate_data,
        pd.DataFrame(
            {
                "year_id": [np.nan],
                "measure": ["year_of_test"],
                "mean": [-1],
            }
        ),
        check_like=True,
    )


def test_country_lvl_aggregate_aggregate_when_measure_has_year_picks_max():
    iu_data = pd.DataFrame(
        {
            "country": ["C1", "C2"] * 2,
            "year_id": [np.nan] * 2 + [2012] * 2,
            "measure": ["M2"] * 2 + [PROB_UNDER_THRESHOLD_MEASURE_NAME] * 2,
            "mean": [15, 12, 0.09, 0.2],
        }
    )
    aggregate_data = aggregation.country_lvl_aggregate(
        iu_data,
        threshold_cols_rename={"M2": "test", "M1": "should not rename"},
        threshold_groupby_cols=["measure"],
        threshold_summary_measure_names=["M2"],
        pct_runs_under_threshold=[0.1],
        denominator_to_use=2,
    )
    pdt.assert_frame_equal(
        aggregate_data,
        pd.DataFrame(
            {
                "year_id": [2012, 2012, np.nan],
                "measure": [
                    "pct_of_ius_with_10pct_runs_under_threshold",
                    "count_of_ius_with_10pct_runs_under_threshold",
                    "year_of_test",
                ],
                "mean": [0.5, 1, 15],
            }
        ),
        check_like=True,
    )


def test_africa_lvl_aggregate_success():
    canonical_ius = [
        pd.DataFrame(
            {
                "scenario": ["scenario_1"],
                "iu_name": "AAA00001",
                "year_id": [2010],
                "measure": ["processed_prevalence"],
                "draw_0": [0.02],
                "draw_1": [0.3],
                "draw_2": [0.4],
                "draw_3": [0.5],
            }
        ),
        pd.DataFrame(
            {
                "scenario": ["scenario_1"],
                "iu_name": "AAA00002",
                "year_id": [2010],
                "measure": ["processed_prevalence"],
                "draw_0": [0.02],
                "draw_1": [0.3],
                "draw_2": [0.4],
                "draw_3": [0.5],
            }
        ),
    ]

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
        canonical_ius,
        composite_africa_data,
        prevalence_threshold=0.05,
        pct_runs_threshold=[0.5, 1.0],
    )
    pdt.assert_frame_equal(
        africa_data,
        pd.DataFrame(
            {
                "scenario": ["scenario_1", "scenario_1", "scenario_1", "scenario_1"],
                "measure": [
                    "M1",
                    "prob_all_ius_under_threshold",
                    "prop_ius_with_50pct_runs_under_threshold",
                    "prop_ius_with_100pct_runs_under_threshold",
                ],
                "year_id": [2010, 2010, 2010, 2010],
                "mean": [0.5, 0.25, 0.0, 0.0],
                "2.5_percentile": [0.215, np.NAN, np.NAN, np.NAN],
                "5_percentile": [0.23, np.NAN, np.NAN, np.NAN],
                "10_percentile": [0.26, np.NAN, np.NAN, np.NAN],
                "25_percentile": [0.35, np.NAN, np.NAN, np.NAN],
                "50_percentile": [0.5, np.NAN, np.NAN, np.NAN],
                "75_percentile": [0.65, np.NAN, np.NAN, np.NAN],
                "90_percentile": [0.74, np.NAN, np.NAN, np.NAN],
                "95_percentile": [0.77, np.NAN, np.NAN, np.NAN],
                "97.5_percentile": [0.785, np.NAN, np.NAN, np.NAN],
                "standard_deviation": [
                    np.std([0.2, 0.4, 0.6, 0.8]),
                    np.NAN,
                    np.NAN,
                    np.NAN,
                ],
                "median": [0.5, np.NAN, np.NAN, np.NAN],
            }
        ),
        check_dtype=False,
    )


def test_africa_lvl_aggregate_multiple_measures_success():
    canonical_ius = [
        pd.DataFrame(
            {
                "scenario": ["scenario_1"],
                "iu_name": "AAA00001",
                "year_id": [2010],
                "measure": ["processed_prevalence"],
                "draw_0": [0.02],
                "draw_1": [0.3],
                "draw_2": [0.4],
                "draw_3": [0.5],
            }
        ),
        pd.DataFrame(
            {
                "scenario": ["scenario_1"],
                "iu_name": "AAA00002",
                "year_id": [2010],
                "measure": ["processed_prevalence"],
                "draw_0": [0.02],
                "draw_1": [0.3],
                "draw_2": [0.4],
                "draw_3": [0.5],
            }
        ),
    ]

    composite_africa_data = pd.DataFrame(
        {
            "year_id": [2010, 2010],
            "scenario": ["scenario_1", "scenario_1"],
            "measure": ["M1", "M2"],
            "draw_0": [0.2, 0.2],
            "draw_1": [0.6, 0.6],
            "draw_2": [0.4, 0.4],
            "draw_3": [0.8, 0.8],
        }
    )

    africa_data = aggregation.africa_lvl_aggregate(
        canonical_ius,
        composite_africa_data,
        prevalence_threshold=0.05,
        pct_runs_threshold=[0.5, 1.0],
    )
    pdt.assert_frame_equal(
        africa_data,
        pd.DataFrame(
            {
                "scenario": ["scenario_1", "scenario_1", "scenario_1", "scenario_1", "scenario_1"],
                "measure": [
                    "M1",
                    "M2",
                    "prob_all_ius_under_threshold",
                    "prop_ius_with_50pct_runs_under_threshold",
                    "prop_ius_with_100pct_runs_under_threshold",
                ],
                "year_id": [2010, 2010, 2010, 2010, 2010],
                "mean": [0.5, 0.5, 0.25, 0.0, 0.0],
                "2.5_percentile": [0.215, 0.215, np.NAN, np.NAN, np.NAN],
                "5_percentile": [0.23, 0.23, np.NAN, np.NAN, np.NAN],
                "10_percentile": [0.26, 0.26, np.NAN, np.NAN, np.NAN],
                "25_percentile": [0.35, 0.35, np.NAN, np.NAN, np.NAN],
                "50_percentile": [0.5, 0.5, np.NAN, np.NAN, np.NAN],
                "75_percentile": [0.65, 0.65, np.NAN, np.NAN, np.NAN],
                "90_percentile": [0.74, 0.74, np.NAN, np.NAN, np.NAN],
                "95_percentile": [0.77, 0.77, np.NAN, np.NAN, np.NAN],
                "97.5_percentile": [0.785, 0.785, np.NAN, np.NAN, np.NAN],
                "standard_deviation": [
                    np.std([0.2, 0.4, 0.6, 0.8]),
                    np.std([0.2, 0.4, 0.6, 0.8]),
                    np.NAN,
                    np.NAN,
                    np.NAN,
                ],
                "median": [0.5, 0.5, np.NAN, np.NAN, np.NAN],
            }
        ),
        check_dtype=False,
    )


def test_calc_count_of_pct_runs_with_thresholds():
    result = aggregation._calc_count_of_pct_runs(pd.Series([0.1, 0.2, 0.3]), pct_of_runs=0.2)
    npt.assert_equal(result, 2)


def test_calc_count_of_pct_runs_with_divisor():
    result = aggregation._calc_count_of_pct_runs(
        pd.Series([0.1, 0.2, 0.3]), pct_of_runs=0.2, denominator_val=3
    )
    npt.assert_equal(result, 2 / 3)


def test_year_all_ius_reach_threshold_with_negative_is_never():
    result = aggregation.year_all_ius_reach_threshold(pd.Series([-1, 2030]))
    npt.assert_equal(result, -1)


def test_year_all_ius_reach_threshold_with_NA_is_never():
    result = aggregation.year_all_ius_reach_threshold(pd.Series([pd.NA, 2030]))
    npt.assert_equal(result, -1)


def test_year_all_ius_reach_threshold_with_only_valid_years_returns_max():
    result = aggregation.year_all_ius_reach_threshold(pd.Series([2035, 2030]))
    npt.assert_equal(result, 2035)

def test_calc_extinction_metrics():
    test_dfs = [
        pd.DataFrame({
            "year_id": [2021.0, 2022.0, 2023.0, 2024.0],
            "scenario": ["scenario_1"] * 4,
            "draw_0": [0.3, 0.2, 0.1, 0]
        }),
        pd.DataFrame({
            "year_id": [2021.0, 2022.0, 2023.0, 2024.0],
            "scenario": ["scenario_1"] * 4,
            "draw_0": [0.4, 0.3, 0.2, 0.1]
        })
    ]
    result = aggregation._calc_extinction_metrics(
        test_dfs,
        extinction_threshold=0.2,
        pct_runs_threshold=[0.5]
    )
    expected = pd.DataFrame({
        "year_id": [2021.0, 2022.0, 2023.0, 2024.0] * 2,
        "scenario": ["scenario_1"] * 8,
        "measure": ["prob_all_ius_under_threshold"] * 4 +
            ["prop_ius_with_50pct_runs_under_threshold"] * 4,
        "mean": [0, 0, 1, 1, 0, 0.5, 1, 1],
    })
    assert {'scenario_1'} == result.keys()
    pdt.assert_frame_equal(
        result["scenario_1"].reset_index(drop=True),
        expected
    )

def test_filter_to_maximum_year_range_for_all_ius_no_nas():
    test_dfs = [
        pd.DataFrame({
            "year_id": [2021.0, 2022.0, 2023.0, 2024.0, 2025.0, None],
            "value": [1, 2, 3, 4, 5, -1]
        }),
        pd.DataFrame({
            "year_id": [2020.0, 2021.0, 2022.0, 2023.0, 2024.0, None],
            "value": [0, 1, 2, 3, 4, -1]
        })
    ]
    result = aggregation.filter_to_maximum_year_range_for_all_ius(test_dfs, keep_na_year_id=False)
    expected = [
        pd.DataFrame({
            "year_id": [2021.0, 2022.0, 2023.0, 2024.0],
            "value": [1, 2, 3, 4]
        }),
        pd.DataFrame({
            "year_id": [2021.0, 2022.0, 2023.0, 2024.0],
            "value": [1, 2, 3, 4]
        })
    ]
    assert len(result) == len(expected)
    for res, exp in zip(result, expected):
        pdt.assert_frame_equal(res, exp)

def test_filter_to_maximum_year_range_for_all_ius_with_nas():
    test_dfs = [
        pd.DataFrame({
            "year_id": [2021.0, 2022.0, 2023.0, 2024.0, 2025.0, None],
            "value": [1, 2, 3, 4, 5, -1]
        }),
        pd.DataFrame({
            "year_id": [2020.0, 2021.0, 2022.0, 2023.0, 2024.0, None],
            "value": [0, 1, 2, 3, 4, -1]
        })
    ]
    result = aggregation.filter_to_maximum_year_range_for_all_ius(test_dfs, keep_na_year_id=True)
    expected = [
        pd.DataFrame({
            "year_id": [2021.0, 2022.0, 2023.0, 2024.0, None],
            "value": [1, 2, 3, 4, -1]
        }),
        pd.DataFrame({
            "year_id": [2021.0, 2022.0, 2023.0, 2024.0, None],
            "value": [1, 2, 3, 4, -1]
        })
    ]
    assert len(result) == len(expected)
    for res, exp in zip(result, expected):
        pdt.assert_frame_equal(res, exp)
