import pandas as pd
import pandas.testing as pdt
from endgame_postprocessing.post_processing import canonicalise
from endgame_postprocessing.post_processing.custom_file_info import CustomFileInfo


def test_canonicalise():
    simple_raw = pd.DataFrame(
        {
            "year_id": [2010, 2011],
            "measure": ["prevalence"] * 2,
            "age_start": [5] * 2,
            "age_end": [100] * 2,
            "draw_0": [0.2] * 2,
        }
    )
    file_info = CustomFileInfo(
        country="AAA",
        file_path="",
        iu="AAA00001",
        scenario="scenario_1",
        scenario_index=1,
        total_scenarios=1,
    )

    canoncial_result = canonicalise.canonicalise_raw(
        simple_raw, file_info, processed_prevalence_name="prevalence"
    )
    pdt.assert_frame_equal(
        canoncial_result,
        pd.DataFrame(
            {
                "scenario": ["scenario_1"] * 2,
                "country_code": ["AAA"] * 2,
                "iu_name": ["AAA00001"] * 2,
                "year_id": [2010, 2011],
                "age_start": [5] * 2,
                "age_end": [100] * 2,
                "measure": ["processed_prevalence"] * 2,
                "draw_0": [0.2] * 2,
            }
        ),
    )


def test_exclude_irrelevant_measure():
    simple_raw = pd.DataFrame(
        {
            "year_id": [2010, 2010, 2011, 2011],
            "age_start": [5] * 4,
            "age_end": [100] * 4,
            "measure": ["prevalence", "measure2"] * 2,
            "draw_0": [0.2, 0.3] * 2,
        }
    )
    file_info = CustomFileInfo(
        country="AAA",
        file_path="",
        iu="AAA00001",
        scenario="scenario_1",
        scenario_index=1,
        total_scenarios=1,
    )

    canoncial_result = canonicalise.canonicalise_raw(
        simple_raw, file_info, processed_prevalence_name="prevalence"
    )
    pdt.assert_frame_equal(
        canoncial_result,
        pd.DataFrame(
            {
                "scenario": ["scenario_1"] * 2,
                "country_code": ["AAA"] * 2,
                "iu_name": ["AAA00001"] * 2,
                "year_id": [2010, 2011],
                "age_start": [5] * 2,
                "age_end": [100] * 2,
                "measure": ["processed_prevalence"] * 2,
                "draw_0": [0.2] * 2,
            }
        ),
    )


def test_exclude_irrelevant_columns():
    simple_raw = pd.DataFrame(
        {
            "year_id": [2010, 2011],
            "measure": ["prevalence"] * 2,
            "age_start": [5] * 2,
            "age_end": [100] * 2,
            "irrelevant": [100] * 2,
            "draw_0": [0.2] * 2,
        }
    )
    file_info = CustomFileInfo(
        country="AAA",
        file_path="",
        iu="AAA00001",
        scenario="scenario_1",
        scenario_index=1,
        total_scenarios=1,
    )

    canoncial_result = canonicalise.canonicalise_raw(
        simple_raw, file_info, processed_prevalence_name="prevalence"
    )
    pdt.assert_frame_equal(
        canoncial_result,
        pd.DataFrame(
            {
                "scenario": ["scenario_1"] * 2,
                "country_code": ["AAA"] * 2,
                "iu_name": ["AAA00001"] * 2,
                "year_id": [2010, 2011],
                "age_start": [5] * 2,
                "age_end": [100] * 2,
                "measure": ["processed_prevalence"] * 2,
                "draw_0": [0.2] * 2,
            }
        ),
    )
