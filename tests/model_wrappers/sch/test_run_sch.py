import pandas as pd
import pandas.testing as pdt
import pytest
from endgame_postprocessing.model_wrappers.sch.run_sch import (
    combine_many_worms,
    get_sth_flat,
    probability_any_worm,
    _check_iu_in_all_folders,
    canonicalise_raw_sch_results,
)
from endgame_postprocessing.post_processing.dataclasses import CustomFileInfo


def test_probability_any_worm_zero_for_all_worms():
    assert probability_any_worm([0.0, 0.0, 0.0]) == 0.0


def test_probability_any_worm_one_for_one_worm():
    assert probability_any_worm([1.0, 0.0, 0.0]) == 1.0


def test_probability_any_worm_half_for_all_worms():
    assert probability_any_worm([0.5, 0.5, 0.5]) == 1.0 - 0.125


def test_combine_many_worms():
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


def test_combine_many_worms_many_years():
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

def test_check_iu_in_all_folders_success_multi_scenario():
    worm_iu_infos = [
        ("worm1", None, "iu1", "scenario1"),
        ("worm2", None, "iu1", "scenario1"),
        ("worm1", None, "iu1", "scenario2"),
        ("worm2", None, "iu1", "scenario2"),
    ]
    _check_iu_in_all_folders(worm_iu_infos)

def test_check_iu_in_all_folders_success_single_burden():
    worm_iu_infos = [
        ("worm1", None, "iu1", "scenario1"),
        ("worm2", "low_burden", "iu1", "scenario1"),
    ]
    _check_iu_in_all_folders(worm_iu_infos)

def test_check_iu_in_all_folders_fail_multi_burden():
    worm_iu_infos = [
        ("worm1", None, "iu1", "scenario1"),
        ("worm2", "low_burden", "iu1", "scenario1"),
        ("worm2", "high_burden", "iu1", "scenario1"),
    ]
    with pytest.raises(Exception):
        _check_iu_in_all_folders(worm_iu_infos)

def test_check_iu_in_all_folders_missing_worm():
    worm_iu_infos = [
        ("worm1", None, "iu1", "scenario1"),
        ("worm1", None, "iu2", "scenario1"),
        ("worm2", "high_burden", "iu2", "scenario1"),
    ]
    with pytest.raises(Exception):
        _check_iu_in_all_folders(worm_iu_infos)


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
