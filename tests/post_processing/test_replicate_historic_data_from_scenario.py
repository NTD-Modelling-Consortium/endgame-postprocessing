import warnings
import pandas as pd
import pandas.testing as pdt
import pytest
from endgame_postprocessing.post_processing.custom_file_info import CustomFileInfo
from endgame_postprocessing.post_processing.replicate_historic_data_from_scenario import replicate_historic_data_in_all_scenarios # noqa E501
from endgame_postprocessing.post_processing import canoncical_columns

def test_replicate_historic_data_in_all_scenarios_one_scenario_copied_to_other():
    source_scenario = pd.DataFrame({
        canoncical_columns.YEAR_ID: [2010, 2011, 2012],
        canoncical_columns.SCENARIO: ["scenario_-1"] * 3,
        "draw_0": [0.1, 0.2, 0.3]
    })
    source_file_info = CustomFileInfo(scenario="scenario_-1", scenario_index=0, total_scenarios=1,
                                      country="AAA", iu="AAA00001", file_path='')
    target_scenario = pd.DataFrame({
        canoncical_columns.YEAR_ID: [2012],
        canoncical_columns.SCENARIO: ["scenario_1"],
        "draw_0": [0.4]
    })
    target_file_info = CustomFileInfo(scenario="scenario_1", scenario_index=0, total_scenarios=1,
                                      country="AAA", iu="AAA00001", file_path='')

    results = {
        "scenario_-1": {"AAA00001": (source_file_info, source_scenario)},
        "scenario_1": {"AAA00001": (target_file_info,target_scenario)},
    }

    modified_results = replicate_historic_data_in_all_scenarios(results, 'scenario_-1')

    assert modified_results["scenario_-1"]["AAA00001"] == (source_file_info, source_scenario)
    modified_target_file_info, modified_target_data  = modified_results["scenario_1"]["AAA00001"]
    assert modified_target_file_info == target_file_info
    pdt.assert_frame_equal(modified_target_data.reset_index(drop=True), pd.DataFrame({
            canoncical_columns.YEAR_ID: [2010, 2011, 2012],
            canoncical_columns.SCENARIO: ["scenario_1"] * 3,
            "draw_0": [0.1, 0.2, 0.4]
    }))

def test_replicate_historic_data_with_non_overlapping_data():
    source_scenario = pd.DataFrame({
        canoncical_columns.YEAR_ID: [2010, 2011, 2012],
        canoncical_columns.SCENARIO: ["scenario_-1"] * 3,
        "draw_0": [0.1, 0.2, 0.3]
    })
    source_file_info = CustomFileInfo(scenario="scenario_-1", scenario_index=0, total_scenarios=1,
                                      country="AAA", iu="AAA00001", file_path='')
    target_scenario = pd.DataFrame({
        canoncical_columns.YEAR_ID: [2014],
        canoncical_columns.SCENARIO: ["scenario_1"],
        "draw_0": [0.4]
    })
    target_file_info = CustomFileInfo(scenario="scenario_1", scenario_index=0, total_scenarios=1,
                                      country="AAA", iu="AAA00001", file_path='')

    results = {
        "scenario_-1": {"AAA00001": (source_file_info, source_scenario)},
        "scenario_1": {"AAA00001": (target_file_info,target_scenario)},
    }

    modified_results = replicate_historic_data_in_all_scenarios(results, 'scenario_-1')

    assert modified_results["scenario_-1"]["AAA00001"] == (source_file_info, source_scenario)
    modified_target_file_info, modified_target_data  = modified_results["scenario_1"]["AAA00001"]
    assert modified_target_file_info == target_file_info
    pdt.assert_frame_equal(modified_target_data.reset_index(drop=True), pd.DataFrame({
            canoncical_columns.YEAR_ID: [2010, 2011, 2012, 2014],
            canoncical_columns.SCENARIO: ["scenario_1"] * 4,
            "draw_0": [0.1, 0.2, 0.3, 0.4]
    }))

def test_replicate_historic_data_missing_forward_iu():
    source_scenario = pd.DataFrame({
        canoncical_columns.YEAR_ID: [2010, 2011, 2012],
        canoncical_columns.SCENARIO: ["scenario_-1"] * 3,
        "draw_0": [0.1, 0.2, 0.3]
    })
    source_file_info = CustomFileInfo(
        scenario="scenario_-1", scenario_index=0, total_scenarios=1,
        country="AAA", iu="AAA00001", file_path='')

    results = {
        "scenario_-1": {"AAA00001": (source_file_info, source_scenario)},
        "scenario_1": {},
    }

    with warnings.catch_warnings(record=True) as w:
        modified_results = replicate_historic_data_in_all_scenarios(results, 'scenario_-1')
        assert [str(warning.message) for warning in w] == [
            "IU AAA00001 found in scenario_-1 but not found in scenario_1"
        ]

    assert modified_results == {"scenario_-1": {}, "scenario_1":{}}

def test_replicate_historic_data_missing_iu_from_history():
    target_scenario = pd.DataFrame({
        canoncical_columns.YEAR_ID: [2012],
        canoncical_columns.SCENARIO: ["scenario_1"],
        "draw_0": [0.4]
    })
    target_file_info = CustomFileInfo(
        scenario="scenario_1", scenario_index=0, total_scenarios=1,
        country="AAA", iu="AAA00001", file_path='')

    results = {
        "scenario_-1": {},
        "scenario_1": {"AAA00001": (target_file_info,target_scenario)},
    }

    with warnings.catch_warnings(record=True) as w:
        modified_results = replicate_historic_data_in_all_scenarios(results, 'scenario_-1')
        assert [str(warning.message) for warning in w] == [
            "IU AAA00001 was not found in scenario_-1 and as such will not have the historic data"
        ]

    assert modified_results == {"scenario_-1": {}, "scenario_1":{}}


def test_replicate_historic_data_missing_iu_from_one_scenario():
    source_scenario = pd.DataFrame({
        canoncical_columns.YEAR_ID: [2010, 2011, 2012],
        canoncical_columns.SCENARIO: ["scenario_-1"] * 3,
        "draw_0": [0.1, 0.2, 0.3]
    })
    source_file_info = CustomFileInfo(scenario="scenario_-1", scenario_index=0, total_scenarios=1,
                                      country="AAA", iu="AAA00001", file_path='')
    target_scenario = pd.DataFrame({
        canoncical_columns.YEAR_ID: [2012],
        canoncical_columns.SCENARIO: ["scenario_1"],
        "draw_0": [0.4]
    })
    target_file_info = CustomFileInfo(scenario="scenario_1", scenario_index=0, total_scenarios=1,
                                      country="AAA", iu="AAA00001", file_path='')

    results = {
        "scenario_-1": {"AAA00001": (source_file_info, source_scenario)},
        "scenario_1": {"AAA00001": (target_file_info,target_scenario)},
        "scenario_2": {},
    }

    modified_results = replicate_historic_data_in_all_scenarios(results, 'scenario_-1')

    assert modified_results == {"scenario_-1": {}, "scenario_1":{}, "scenario_2":{}}


def test_replicate_historic_data_missing_iu_from_one_scenario_different_iu_present():
    source_scenario1 = pd.DataFrame({
        canoncical_columns.YEAR_ID: [2010, 2011, 2012],
        canoncical_columns.SCENARIO: ["scenario_-1"] * 3,
        "draw_0": [0.1, 0.2, 0.3]
    })
    source_file_info1 = CustomFileInfo(scenario="scenario_-1", scenario_index=0, total_scenarios=1,
                                      country="AAA", iu="AAA00001", file_path='')

    source_scenario2 = pd.DataFrame({
        canoncical_columns.YEAR_ID: [2010, 2011, 2012],
        canoncical_columns.SCENARIO: ["scenario_-1"] * 3,
        "draw_0": [0.1, 0.2, 0.3]
    })
    source_file_info2 = CustomFileInfo(scenario="scenario_-1", scenario_index=0, total_scenarios=1,
                                      country="AAA", iu="AAA00002", file_path='')

    scenario1_data1 = pd.DataFrame({
        canoncical_columns.YEAR_ID: [2012],
        canoncical_columns.SCENARIO: ["scenario_1"],
        "draw_0": [0.4]
    })
    scenario1_data2 = pd.DataFrame({
        canoncical_columns.YEAR_ID: [2012],
        canoncical_columns.SCENARIO: ["scenario_1"],
        "draw_0": [0.4]
    })
    scenario2_data2 = pd.DataFrame({
        canoncical_columns.YEAR_ID: [2012],
        canoncical_columns.SCENARIO: ["scenario_2"],
        "draw_0": [0.5]
    })
    scenario1_file_info1 = CustomFileInfo(scenario="scenario_1", scenario_index=0, total_scenarios=1,
                                      country="AAA", iu="AAA00001", file_path='')
    scenario1_file_info2 = CustomFileInfo(scenario="scenario_1", scenario_index=0, total_scenarios=1,
                                      country="AAA", iu="AAA00002", file_path='')
    scenario2_file_info2 = CustomFileInfo(scenario="scenario_2", scenario_index=0, total_scenarios=1,
                                      country="AAA", iu="AAA00002", file_path='')


    results = {
        "scenario_-1": {"AAA00001": (source_file_info1, source_scenario1),
                        "AAA00002": (source_file_info2, source_scenario2)},
        "scenario_1": {"AAA00001": (scenario1_file_info1, scenario1_data1),
                       "AAA00002": (scenario1_file_info2, scenario1_data2)},
        "scenario_2": {"AAA00002": (scenario2_file_info2, scenario2_data2)},
    }

    modified_results = replicate_historic_data_in_all_scenarios(results, 'scenario_-1')

    assert modified_results["scenario_-1"]["AAA00002"] == (source_file_info2, source_scenario2)
    assert "AAA00001" not in modified_results["scenario_-1"]

    scenario1_modified_file_info, scenario1_modified_data  = modified_results["scenario_1"]["AAA00002"]
    assert scenario1_modified_file_info == scenario1_file_info2
    pdt.assert_frame_equal(scenario1_modified_data.reset_index(drop=True), pd.DataFrame({
            canoncical_columns.YEAR_ID: [2010, 2011, 2012],
            canoncical_columns.SCENARIO: ["scenario_1"] * 3,
            "draw_0": [0.1, 0.2, 0.4]}))

    scenario_2_modified_target_file_info, scenario_2_modified_target_data  = modified_results["scenario_2"]["AAA00002"]
    assert scenario_2_modified_target_file_info == scenario2_file_info2
    pdt.assert_frame_equal(scenario_2_modified_target_data.reset_index(drop=True), pd.DataFrame({
            canoncical_columns.YEAR_ID: [2010, 2011, 2012],
            canoncical_columns.SCENARIO: ["scenario_2"] * 3,
            "draw_0": [0.1, 0.2, 0.5]}))

def test_replicate_historic_data_in_all_scenarios_scenario_missing_raises_exception():
    with pytest.raises(Exception) as e:
        _ = replicate_historic_data_in_all_scenarios(
            {"scenario_1": {}}, 'scenario_-1')
    assert e.match(r"Invalid source_scenario: 'scenario_-1' as not in \['scenario_1'\]")
