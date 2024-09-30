import pandas as pd
import pandas.testing as pdt
from endgame_postprocessing.post_processing import composite_run


def test_build_iu_case_numbers():
    canoncial_iu = pd.DataFrame({"draw_1": [0.2, 0.3], "draw_2": [0.3, 0.4]})
    result = composite_run.build_iu_case_numbers(canoncial_iu, population=100)
    pdt.assert_frame_equal(
        result, pd.DataFrame({"draw_1": [20.0, 30.0], "draw_2": [30.0, 40.0]})
    )


def test_build_composite_run_from_one_iu():
    canoncial_iu = pd.DataFrame(
        {
            "iu_code": ["AAA00001"] * 2,
            "year_id": [2010, 2011],
            "draw_1": [0.2, 0.3],
            "draw_2": [0.3, 0.4],
        }
    )
    population_data = {"AAA00001": 100}
    result = composite_run.build_composite_run([canoncial_iu], population_data)
    pdt.assert_frame_equal(
        result,
        pd.DataFrame(
            {
                "year_id": [2010, 2011],
                "draw_1": [20.0, 30.0],
                "draw_2": [30.0, 40.0],
            }
        ),
    )


def test_build_composite_run_from_two_iu_but_second_iu_ignored():
    canoncial_iu1 = pd.DataFrame(
        {
            "year_id": [2010, 2011],
            "iu_code": ["AAA00001"] * 2,
            "draw_1": [0.2, 0.3],
            "draw_2": [0.3, 0.4],
        }
    )
    canoncial_iu2 = pd.DataFrame(
        {
            "year_id": [2010, 2011],
            "iu_code": ["AAA00002"] * 2,
            "draw_1": [0.8, 0.9],
            "draw_2": [0.8, 0.9],
        }
    )
    population_data = {"AAA00001": 100, "AAA00002": 0}
    result = composite_run.build_composite_run(
        [canoncial_iu1, canoncial_iu2], population_data
    )
    pdt.assert_frame_equal(
        result,
        pd.DataFrame(
            {"year_id": [2010, 2011], "draw_1": [20.0, 30.0], "draw_2": [30.0, 40.0]}
        ),
    )


def test_build_composite_run_from_two_equal_sized_ius():
    canoncial_iu1 = pd.DataFrame(
        {"year_id": [2010], "iu_code": ["AAA00001"], "draw_1": [0.2], "draw_2": [0.3]}
    )
    canoncial_iu2 = pd.DataFrame(
        {"year_id": [2010], "iu_code": ["AAA00002"], "draw_1": [0.8], "draw_2": [0.9]}
    )
    population_data = {"AAA00001": 10, "AAA00002": 10}
    result = composite_run.build_composite_run(
        [canoncial_iu1, canoncial_iu2], population_data
    )
    pdt.assert_frame_equal(
        result, pd.DataFrame({"year_id": [2010], "draw_1": [10.0], "draw_2": [12.0]})
    )


def test_build_composite_run_retains_year_id():
    canoncial_iu1 = pd.DataFrame(
        {
            "iu_code": ["AAA00001"] * 2,
            "year_id": [2010, 2011],
            "draw_1": [0.2] * 2,
            "draw_2": [0.3] * 2,
        }
    )
    canoncial_iu2 = pd.DataFrame(
        {
            "iu_code": ["AAA00002"] * 2,
            "year_id": [2010, 2011],
            "draw_1": [0.8] * 2,
            "draw_2": [0.9] * 2,
        }
    )
    population_data = {"AAA00001": 10, "AAA00002": 10}
    result = composite_run.build_composite_run(
        [canoncial_iu1, canoncial_iu2], population_data
    )
    pdt.assert_frame_equal(
        result,
        pd.DataFrame(
            {"year_id": [2010, 2011], "draw_1": [10.0] * 2, "draw_2": [12.0] * 2}
        ),
    )
