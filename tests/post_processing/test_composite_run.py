import pandas as pd
import pandas.testing as pdt

from endgame_postprocessing.post_processing import composite_run
from endgame_postprocessing.post_processing.disease import Disease
from endgame_postprocessing.post_processing.iu_data import IUData, IUSelectionCriteria


def test_build_iu_case_numbers():
    canoncial_iu = pd.DataFrame({"draw_0": [0.2, 0.3], "draw_1": [0.3, 0.4]})
    result = composite_run.build_iu_case_numbers(canoncial_iu, population=100)
    pdt.assert_frame_equal(
        result, pd.DataFrame({"draw_0": [20.0, 30.0], "draw_1": [30.0, 40.0]})
    )


def test_build_composite_run_from_one_iu():
    canoncial_iu = pd.DataFrame(
        {
            "iu_name": ["AAA00001"] * 2,
            "scenario": ["scenario_1"] * 2,
            "country_code": ["AAA"] * 2,
            "measure": ["processed_prevalence"] * 2,
            "year_id": [2010, 2011],
            "draw_0": [0.2, 0.3],
            "draw_1": [0.3, 0.4],
        }
    )
    population_data = IUData(
        pd.DataFrame(
            {
                "IU_CODE": ["AAA00001"],
                "ADMIN0ISO3": ["AAA"],
                "Priority_Population_LF": [100],
            }
        ),
        disease=Disease.LF,
        iu_selection_criteria=IUSelectionCriteria.ALL_IUS,
    )
    result = composite_run.build_composite_run([canoncial_iu], population_data)
    pdt.assert_frame_equal(
        result,
        pd.DataFrame(
            {
                "year_id": [2010, 2011],
                "scenario": ["scenario_1"] * 2,
                "country_code": ["AAA"] * 2,
                "measure": ["processed_prevalence"] * 2,
                "draw_0": [0.2, 0.3],
                "draw_1": [0.3, 0.4],
            }
        ),
    )


def test_build_composite_run_from_two_iu_but_second_iu_ignored():
    canoncial_iu1 = pd.DataFrame(
        {
            "year_id": [2010, 2011],
            "scenario": ["scenario_1"] * 2,
            "country_code": ["AAA"] * 2,
            "measure": ["processed_prevalence"] * 2,
            "iu_name": ["AAA00001"] * 2,
            "draw_0": [0.2, 0.3],
            "draw_1": [0.3, 0.4],
        }
    )
    canoncial_iu2 = pd.DataFrame(
        {
            "year_id": [2010, 2011],
            "scenario": ["scenario_1"] * 2,
            "country_code": ["AAA"] * 2,
            "measure": ["processed_prevalence"] * 2,
            "iu_name": ["AAA00002"] * 2,
            "draw_0": [0.8, 0.9],
            "draw_1": [0.8, 0.9],
        }
    )
    population_data = IUData(
        pd.DataFrame(
            {
                "IU_CODE": ["AAA00001", "AAA00002"],
                "ADMIN0ISO3": ["AAA"] * 2,
                "Priority_Population_LF": [100, 0],
            }
        ),
        disease=Disease.LF,
        iu_selection_criteria=IUSelectionCriteria.ALL_IUS,
    )
    result = composite_run.build_composite_run(
        [canoncial_iu1, canoncial_iu2], population_data
    )
    pdt.assert_frame_equal(
        result,
        pd.DataFrame(
            {
                "year_id": [2010, 2011],
                "scenario": ["scenario_1"] * 2,
                "country_code": ["AAA"] * 2,
                "measure": ["processed_prevalence"] * 2,
                "draw_0": [0.2, 0.3],
                "draw_1": [0.3, 0.4],
            }
        ),
    )


def test_build_composite_run_from_two_equal_sized_ius():
    canoncial_iu1 = pd.DataFrame(
        {
            "year_id": [2010],
            "scenario": ["scenario_1"],
            "country_code": ["AAA"],
            "measure": ["processed_prevalence"],
            "iu_name": ["AAA00001"],
            "draw_0": [0.2],
            "draw_1": [0.3],
        }
    )
    canoncial_iu2 = pd.DataFrame(
        {
            "year_id": [2010],
            "scenario": ["scenario_1"],
            "country_code": ["AAA"],
            "measure": ["processed_prevalence"],
            "iu_name": ["AAA00002"],
            "draw_0": [0.8],
            "draw_1": [0.9],
        }
    )
    population_data = IUData(
        pd.DataFrame(
            {
                "IU_CODE": ["AAA00001", "AAA00002"],
                "ADMIN0ISO3": ["AAA"] * 2,
                "Priority_Population_LF": [10, 10],
            }
        ),
        disease=Disease.LF,
        iu_selection_criteria=IUSelectionCriteria.ALL_IUS,
    )
    result = composite_run.build_composite_run(
        [canoncial_iu1, canoncial_iu2], population_data
    )
    pdt.assert_frame_equal(
        result,
        pd.DataFrame(
            {
                "year_id": [2010],
                "scenario": ["scenario_1"],
                "country_code": ["AAA"],
                "measure": ["processed_prevalence"],
                "draw_0": [10.0 / 20.0],
                "draw_1": [12.0 / 20.0],
            }
        ),
    )


def test_build_composite_run_retains_year_id():
    canoncial_iu1 = pd.DataFrame(
        {
            "iu_name": ["AAA00001"] * 2,
            "scenario": ["scenario_1"] * 2,
            "country_code": ["AAA"] * 2,
            "measure": ["processed_prevalence"] * 2,
            "year_id": [2010, 2011],
            "draw_0": [0.2] * 2,
            "draw_1": [0.3] * 2,
        }
    )
    canoncial_iu2 = pd.DataFrame(
        {
            "iu_name": ["AAA00002"] * 2,
            "scenario": ["scenario_1"] * 2,
            "country_code": ["AAA"] * 2,
            "measure": ["processed_prevalence"] * 2,
            "year_id": [2010, 2011],
            "draw_0": [0.8] * 2,
            "draw_1": [0.9] * 2,
        }
    )
    population_data = IUData(
        pd.DataFrame(
            {
                "IU_CODE": ["AAA00001", "AAA00002"],
                "ADMIN0ISO3": ["AAA"] * 2,
                "Priority_Population_LF": [10, 10],
            }
        ),
        disease=Disease.LF,
        iu_selection_criteria=IUSelectionCriteria.ALL_IUS,
    )
    result = composite_run.build_composite_run(
        [canoncial_iu1, canoncial_iu2], population_data
    )
    pdt.assert_frame_equal(
        result,
        pd.DataFrame(
            {
                "year_id": [2010, 2011],
                "scenario": ["scenario_1"] * 2,
                "country_code": ["AAA"] * 2,
                "measure": ["processed_prevalence"] * 2,
                "draw_0": [10.0 / 20.0] * 2,
                "draw_1": [12.0 / 20.0] * 2,
            }
        ),
    )


def test_build_composite_multiple_scenarios():
    canoncial_iu_scenario_1 = pd.DataFrame(
        {
            "iu_name": ["AAA00001"] * 2,
            "scenario": ["scenario_1"] * 2,
            "country_code": ["AAA"] * 2,
            "measure": ["processed_prevalence"] * 2,
            "year_id": [2010, 2011],
            "draw_0": [0.2] * 2,
            "draw_1": [0.3] * 2,
        }
    )
    canoncial_iu_scenario_2 = pd.DataFrame(
        {
            "iu_name": ["AAA00001"] * 2,
            "scenario": ["scenario_2"] * 2,
            "country_code": ["AAA"] * 2,
            "measure": ["processed_prevalence"] * 2,
            "year_id": [2010, 2011],
            "draw_0": [0.8] * 2,
            "draw_1": [0.9] * 2,
        }
    )
    population_data = IUData(
        pd.DataFrame(
            {
                "IU_CODE": ["AAA00001"],
                "ADMIN0ISO3": ["AAA"],
                "Priority_Population_LF": [10],
            }
        ),
        disease=Disease.LF,
        iu_selection_criteria=IUSelectionCriteria.ALL_IUS,
    )
    result = composite_run.build_composite_run_multiple_scenarios([canoncial_iu_scenario_1, canoncial_iu_scenario_2],
                                                                  population_data)
    pdt.assert_frame_equal(
        result,
        pd.DataFrame(
            {
                "year_id": [2010, 2011, 2010, 2011],
                "scenario": ["scenario_1", "scenario_1", "scenario_2", "scenario_2"],
                "country_code": ["AAA"] * 4,
                "measure": ["processed_prevalence"] * 4,
                "draw_0": [0.2, 0.2, 0.8, 0.8],
                "draw_1": [0.3, 0.3, 0.9, 0.9],
            }
        ),
    )
