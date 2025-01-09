import warnings
import pandas as pd
import pandas.testing as pdt
import pytest
from endgame_postprocessing.post_processing.custom_file_info import CustomFileInfo
from endgame_postprocessing.post_processing.replicate_historic_data_from_scenario import replicate_historic_data_in_all_scenarios # noqa E501
from endgame_postprocessing.post_processing import canoncical_columns

def _generate_canonical_result(scenario, years, prevalence, iu):
    source_scenario = pd.DataFrame({
        canoncical_columns.YEAR_ID: years,
        canoncical_columns.SCENARIO: [scenario] * len(years),
        "draw_0": prevalence
    })
    file_info = CustomFileInfo(scenario=scenario, scenario_index=0, total_scenarios=1,
                                      country=iu[:3], iu=iu, file_path='')
    return (file_info, source_scenario)

def _assert_results_match(results, expected_results):
    assert results.keys() == expected_results.keys()
    for scenario in results:
        assert results[scenario].keys() == expected_results[scenario].keys()
        for iu in results[scenario]:
            result_file_info, result_data = results[scenario][iu]
            expected_file_info, expected_data = expected_results[scenario][iu]
            assert result_file_info == expected_file_info
            pdt.assert_frame_equal(result_data.reset_index(drop=True), expected_data)

def test_replicate_historic_data_in_all_scenarios_one_scenario_copied_to_other():
    source_result  = _generate_canonical_result(
        "scenario_-1", years = [2010, 2011, 2012], prevalence=[0.1, 0.2, 0.3], iu="AAA00001")
    (target_file_info, target_data) = _generate_canonical_result(
        "scenario_1", years=[2012], prevalence=[0.4], iu="AAA00001")

    results = {
        "scenario_-1": {"AAA00001": source_result},
        "scenario_1": {"AAA00001": (target_file_info, target_data)},
    }

    modified_results = replicate_historic_data_in_all_scenarios(results, 'scenario_-1')

    _assert_results_match(modified_results, {
        "scenario_-1": { "AAA00001": source_result},
        "scenario_1": { "AAA00001": (target_file_info, pd.DataFrame({
            canoncical_columns.YEAR_ID: [2010, 2011, 2012],
            canoncical_columns.SCENARIO: ["scenario_1"] * 3,
            "draw_0": [0.1, 0.2, 0.4]
    }))}})

def test_replicate_historic_data_with_non_overlapping_data():
    source_result  = _generate_canonical_result(
        "scenario_-1", years = [2010, 2011, 2012], prevalence=[0.1, 0.2, 0.3], iu="AAA00001")
    (target_file_info, target_data) = _generate_canonical_result(
        "scenario_1", years=[2014], prevalence=[0.4], iu="AAA00001")

    results = {
        "scenario_-1": {"AAA00001": source_result},
        "scenario_1": {"AAA00001": (target_file_info,target_data)},
    }

    modified_results = replicate_historic_data_in_all_scenarios(results, 'scenario_-1')
    _assert_results_match(modified_results, {
        "scenario_-1": { "AAA00001": source_result},
        "scenario_1": { "AAA00001": (target_file_info, pd.DataFrame({
            canoncical_columns.YEAR_ID: [2010, 2011, 2012, 2014],
            canoncical_columns.SCENARIO: ["scenario_1"] * 4,
            "draw_0": [0.1, 0.2, 0.3, 0.4]
    }))}})

def test_replicate_historic_data_missing_forward_iu():
    source_result  = _generate_canonical_result(
        "scenario_-1", years = [2010, 2011, 2012], prevalence=[0.1, 0.2, 0.3], iu="AAA00001")
    results = {
        "scenario_-1": {"AAA00001": source_result},
        "scenario_1": {},
    }

    with warnings.catch_warnings(record=True) as w:
        modified_results = replicate_historic_data_in_all_scenarios(results, 'scenario_-1')
        assert [str(warning.message) for warning in w] == [
            "IU AAA00001 found in scenario_-1 but not found in scenario_1"
        ]

    assert modified_results == {"scenario_-1": {}, "scenario_1":{}}

def test_replicate_historic_data_missing_iu_from_history():
    (target_file_info, target_data) = _generate_canonical_result(
        "scenario_1", years=[2012], prevalence=[0.4], iu="AAA00001")

    results = {
        "scenario_-1": {},
        "scenario_1": {"AAA00001": (target_file_info,target_data)},
    }

    with warnings.catch_warnings(record=True) as w:
        modified_results = replicate_historic_data_in_all_scenarios(results, 'scenario_-1')
        assert [str(warning.message) for warning in w] == [
            "IU AAA00001 was not found in scenario_-1 and as such will not have the historic data"
        ]

    assert modified_results == {"scenario_-1": {}, "scenario_1":{}}


def test_replicate_historic_data_missing_iu_from_one_scenario():
    source_result = _generate_canonical_result(
        "scenario_-1", years=[2010, 2011, 2012], prevalence=[0.1, 0.2, 0.3], iu="AAA00001")
    target_result = _generate_canonical_result(
        "scenario_1", years=[2012], prevalence=[0.4], iu="AAA00001")

    results = {
        "scenario_-1": {"AAA00001": source_result},
        "scenario_1": {"AAA00001": target_result},
        "scenario_2": {},
    }

    modified_results = replicate_historic_data_in_all_scenarios(results, 'scenario_-1')

    assert modified_results == {"scenario_-1": {}, "scenario_1":{}, "scenario_2":{}}


def test_replicate_historic_data_missing_iu_from_one_scenario_different_iu_present():
    source_result1 = _generate_canonical_result(
        "scenario_-1", years=[2010, 2011, 2012], prevalence=[0.1, 0.2, 0.3], iu="AAA00001")

    source_result2 = _generate_canonical_result(
        "scenario_-1", years=[2010, 2011, 2012], prevalence=[0.1, 0.2, 0.3], iu="AAA00002")

    scenario1_result1 = _generate_canonical_result(
        "scenario_1", years=[2012], prevalence=[0.4], iu="AAA00001")
    scenario1_result2 = _generate_canonical_result(
        "scenario_1", years=[2012], prevalence=[0.4], iu="AAA00002")
    scenario2_result2 = _generate_canonical_result(
        "scenario_2", years=[2012], prevalence=[0.5], iu="AAA00002")

    results = {
        "scenario_-1": {"AAA00001": source_result1,
                        "AAA00002": source_result2},
        "scenario_1": {"AAA00001": scenario1_result1,
                       "AAA00002": scenario1_result2},
        "scenario_2": {"AAA00002": scenario2_result2},
    }

    modified_results = replicate_historic_data_in_all_scenarios(results, 'scenario_-1')

    _assert_results_match(modified_results, {
        "scenario_-1": {"AAA00002": source_result2},
        "scenario_1": {"AAA00002": (scenario1_result2[0], pd.DataFrame({
            canoncical_columns.YEAR_ID: [2010, 2011, 2012],
            canoncical_columns.SCENARIO: ["scenario_1"] * 3,
            "draw_0": [0.1, 0.2, 0.4]}))},
        "scenario_2": {"AAA00002": (scenario2_result2[0], pd.DataFrame({
            canoncical_columns.YEAR_ID: [2010, 2011, 2012],
            canoncical_columns.SCENARIO: ["scenario_2"] * 3,
            "draw_0": [0.1, 0.2, 0.5]}))},
    })

def test_replicate_historic_data_in_all_scenarios_scenario_missing_raises_exception():
    with pytest.raises(Exception) as e:
        _ = replicate_historic_data_in_all_scenarios(
            {"scenario_1": {}}, 'scenario_-1')
    assert e.match(r"Invalid source_scenario: 'scenario_-1' as not in \['scenario_1'\]")
