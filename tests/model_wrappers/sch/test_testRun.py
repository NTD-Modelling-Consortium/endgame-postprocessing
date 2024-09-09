import pandas as pd
import pandas.testing as pdt
from endgame_postprocessing.model_wrappers.sch.testRun import recombine_ages


def test_recombine_ages_unique_measures_unaffected():
    input = pd.DataFrame(
        {
            "Time": [0, 1],
            "age_start": [0, 0],
            "age_end": [1, 1],
            "intensity": ["normal", "high"],
            "measure": ["prevalance", "prevalance"],
            "draw_1": [3, 1],
        }
    )

    output = recombine_ages(input)
    # TODO: why is dtype wrong
    pdt.assert_frame_equal(output, input, check_dtype=False)


def test_recombine_ages_two_age_groups_summed():
    input = pd.DataFrame(
        {
            "Time": [0, 0],
            "intensity": ["normal", "normal"],
            "age_start": [0, 1],
            "age_end": [1, 2],
            "measure": ["prevalance", "prevalance"],
            "draw_1": [3, 1],
        }
    )

    output = recombine_ages(input)
    pdt.assert_frame_equal(
        output,
        pd.DataFrame(
            {
                "Time": [0],
                "age_start": [0],
                "age_end": [2],
                "intensity": ["normal"],
                "measure": ["prevalance"],
                "draw_1": [4],
            }
        ),
        check_dtype=False,
        check_like=True,
    )
