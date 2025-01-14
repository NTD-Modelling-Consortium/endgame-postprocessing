import os
import pandas as pd
import pandas.testing as pdt
import pytest
from endgame_postprocessing.model_wrappers.sch.run_sch import (
    canoncialise_single_result,
    combine_many_worms,
    get_sth_flat,
    probability_any_worm,
    _check_iu_in_all_folders,
    canonicalise_raw_sch_results,
    probability_any_worm_max,
    rename_historic_file,
)
from endgame_postprocessing.post_processing.custom_file_info import CustomFileInfo

def test_probability_any_worm_zero_for_all_worms():
    assert probability_any_worm([0.0, 0.0, 0.0]) == 0.0


def test_probability_any_worm_one_for_one_worm():
    assert probability_any_worm([1.0, 0.0, 0.0]) == 1.0


def test_probability_any_worm_half_for_all_worms():
    assert probability_any_worm([0.5, 0.5, 0.5]) == 1.0 - 0.125


def test_probability_any_worm_max_same_prev():
    assert probability_any_worm_max([0.5, 0.5, 0.5]) == 0.5


def test_probability_any_worm_diff_prev():
    assert probability_any_worm_max([0.5, 0.7, 0.3]) == 0.7


def test_combine_many_worms_except_not_callable():
    with pytest.raises(Exception):
        combine_many_worms([], [], combination_function="123")

def test_combine_many_worms_default_combination():
    first_worm = pd.DataFrame(
        {
            "year": [2010],
            "draw_0": [0.5],
            "draw_1": [0.0],
        }
    )

    second_worm = pd.DataFrame(
        {
            "year": [2010],
            "draw_0": [0.5],
            "draw_1": [0.0],
        }
    )

    third_worm = pd.DataFrame(
        {
            "year": [2010],
            "draw_0": [0.5],
            "draw_1": [1.0],
        }
    )
    pdt.assert_frame_equal(
        combine_many_worms(first_worm, [second_worm, third_worm]),
        pd.DataFrame(
            {
                "year": [2010],
                "draw_0": [1 - 0.125],
                "draw_1": [1.0],
            }
        ),
    )

def test_combine_many_worms_default_combination_empty_df():
    first_worm = pd.DataFrame(
        {
            "year": [2010],
            "draw_0": [0.5],
            "draw_1": [1.0],
        }
    )

    second_worm = pd.DataFrame(
        {
            "year": [2010],
            "draw_0": [0.5],
            "draw_1": [0.0],
        }
    )

    third_worm = pd.DataFrame()

    pdt.assert_frame_equal(
        combine_many_worms(first_worm, [second_worm, third_worm]),
        pd.DataFrame(
            {
                "year": [2010],
                "draw_0": [1 - 0.25],
                "draw_1": [1.0],
            }
        ),
    )


def test_combine_many_worms_default_combination_many_years():
    first_worm = pd.DataFrame(
        {
            "year": [2010, 2011],
            "draw_0": [0.5, 0.5],
            "draw_1": [0.0, 0.0],
        }
    )

    second_worm = pd.DataFrame(
        {
            "year": [2010, 2011],
            "draw_0": [0.5, 0.5],
            "draw_1": [0.0, 0.0],
        }
    )

    third_worm = pd.DataFrame(
        {
            "year": [2010, 2011],
            "draw_0": [0.5, 0.5],
            "draw_1": [1.0, 0.0],
        }
    )
    pdt.assert_frame_equal(
        combine_many_worms(first_worm, [second_worm, third_worm]),
        pd.DataFrame(
            {
                "year": [2010, 2011],
                "draw_0": [1 - 0.125] * 2,
                "draw_1": [1.0, 0.0],
            }
        ),
    )

def test_combine_many_worms_max_combination():
    first_worm = pd.DataFrame(
        {
            "year": [2010],
            "draw_0": [0.5],
            "draw_1": [0.7],
        }
    )

    second_worm = pd.DataFrame(
        {
            "year": [2010],
            "draw_0": [0.5],
            "draw_1": [0.3],
        }
    )

    pdt.assert_frame_equal(
        combine_many_worms(
            first_worm, [second_worm],
            combination_function=probability_any_worm_max
        ),
        pd.DataFrame(
            {
                "year": [2010],
                "draw_0": [0.5],
                "draw_1": [0.7],
            }
        ),
    )

def test_combine_many_worms_max_combination_empty_df():
    first_worm = pd.DataFrame(
        {
            "year": [2010],
            "draw_0": [0.5],
            "draw_1": [1.0],
        }
    )

    second_worm = pd.DataFrame()

    pdt.assert_frame_equal(
        combine_many_worms(
            first_worm, [second_worm],
            combination_function=probability_any_worm_max
        ),
        pd.DataFrame(
            {
                "year": [2010],
                "draw_0": [0.5],
                "draw_1": [1.0],
            }
        ),
    )


def test_combine_many_worms_max_combination_many_years():
    first_worm = pd.DataFrame(
        {
            "year": [2010, 2011],
            "draw_0": [0.5, 0.5],
            "draw_1": [0.3, 0.0],
        }
    )

    second_worm = pd.DataFrame(
        {
            "year": [2010, 2011],
            "draw_0": [0.5, 0.3],
            "draw_1": [0.0, 0.7],
        }
    )

    pdt.assert_frame_equal(
        combine_many_worms(
            first_worm, [second_worm],
            combination_function=probability_any_worm_max
        ),
        pd.DataFrame(
            {
                "year": [2010, 2011],
                "draw_0": [0.5, 0.5],
                "draw_1": [0.3, 0.7],
            }
        ),
    )

def test_canoncialise_single_result_no_file_warning():
    with pytest.warns():
        canoncialise_single_result(CustomFileInfo(
            scenario_index=1,
            total_scenarios=1,
            scenario="scenario_1",
            country="TST",
            iu="TST01234",
            file_path="random/file.csv",
        ), warning_if_no_file=True)

def test_canoncialise_single_result_no_file_exception_default():
    with pytest.raises(Exception):
        canoncialise_single_result(CustomFileInfo(
            scenario_index=1,
            total_scenarios=1,
            scenario="scenario_1",
            country="TST",
            iu="TST01234",
            file_path="random/file.csv",
        ))

def test_check_iu_in_all_folders_success_multi_scenario():
    worm_iu_infos = [
        ("worm1", None, "iu1", "scenario1"),
        ("worm2", None, "iu1", "scenario1"),
        ("worm1", None, "iu1", "scenario2"),
        ("worm2", None, "iu1", "scenario2"),
    ]
    _check_iu_in_all_folders(worm_iu_infos, warning_if_no_file=False)

def test_check_iu_in_all_folders_success_single_burden():
    worm_iu_infos = [
        ("worm1", None, "iu1", "scenario1"),
        ("worm2", "low_burden", "iu1", "scenario1"),
    ]
    _check_iu_in_all_folders(worm_iu_infos, warning_if_no_file=False)

def test_check_iu_in_all_folders_fail_multi_burden():
    worm_iu_infos = [
        ("worm1", None, "iu1", "scenario1"),
        ("worm2", "low_burden", "iu1", "scenario1"),
        ("worm2", "high_burden", "iu1", "scenario1"),
    ]
    with pytest.raises(Exception):
        _check_iu_in_all_folders(worm_iu_infos, warning_if_no_file=False)

def test_check_iu_in_all_folders_missing_worm_exception():
    worm_iu_infos = [
        ("worm1", None, "iu1", "scenario1"),
        ("worm1", None, "iu2", "scenario1"),
        ("worm2", "high_burden", "iu2", "scenario1"),
    ]
    with pytest.raises(Exception):
        _check_iu_in_all_folders(worm_iu_infos, warning_if_no_file=False)

def test_check_iu_in_all_folders_missing_worm_warning():
    worm_iu_infos = [
        ("worm1", None, "iu1", "scenario1"),
        ("worm1", None, "iu2", "scenario1"),
        ("worm2", "high_burden", "iu2", "scenario1"),
    ]
    with pytest.warns():
        _check_iu_in_all_folders(worm_iu_infos, warning_if_no_file=True)


def test_canonicalise_raw_sch_results_all_worm_no_worm_directory():
    with pytest.raises(Exception):
        canonicalise_raw_sch_results("test_input_dir", "test_output_dir", worm_directories = [])


def test_flat_walk(fs):
    fs.create_file(
        "foo/ntdmc-AGO02049-hookworm-group_001-scenario_2a-group_001-200_simulations.csv"
    )
    results = list(get_sth_flat("foo"))
    assert results == [
        CustomFileInfo(
            scenario_index=1,
            total_scenarios=3,  # TODO
            scenario="scenario_2a",
            country="AGO",
            iu="AGO02049",
            file_path="foo/ntdmc-AGO02049-hookworm-group_001-scenario_2a-group_001-200_simulations.csv",
        )
    ]


def test_rename_flat_historic_data(fs):
    fs.create_file(
        "foo/PrevDataset_Hook_AAA00001.csv"),
    rename_historic_file("foo", "bar")
    assert os.path.exists("bar/ntdmc-AAA00001-hookworm-group_001-scenario_0_survey_type_kk2-group_001-200_simulations.csv")
    assert os.path.exists("foo/PrevDataset_Hook_AAA00001.csv")
