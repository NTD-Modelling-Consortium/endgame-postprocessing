import pandas as pd
import pandas.testing as pdt
from endgame_postprocessing.model_wrappers.sch.run_sch import (
    combine_many_worms,
    get_sth_flat,
    probability_any_worm,
    read_remapping_file,
    remap_old_ius_to_new_ius,
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


def test_read_remapping_file():
    input = pd.DataFrame(
        {
            "IU_CODE": ["AAA0", "AAA0", "BBB0"],
            "ADMIN0ISO3": ["AAA", "AAA", "BBB"],
            "ESPEN_IU_ID": [1.0, 2.0, 1.0],
        }
    )
    output = read_remapping_file(input)
    assert output == {"AAA0": ["AAA1", "AAA2"], "BBB0": ["BBB1"]}


def test_read_remapping_file_with_nan():
    input = pd.DataFrame(
        {
            "IU_CODE": ["AAA0"],
            "ADMIN0ISO3": ["AAA"],
            "ESPEN_IU_ID": [pd.NA],
        }
    )
    output = read_remapping_file(input)
    assert output == {}


def test_read_remapping_file_with_nan_in_irrelevant_column():
    input = pd.DataFrame(
        {
            "IU_CODE": ["AAA0"],
            "ADMIN0ISO3": ["AAA"],
            "ESPEN_IU_ID": [1.0],
            "another_column": [pd.NA],
        }
    )
    output = read_remapping_file(input)
    assert output == {"AAA0": ["AAA1"]}


def test_remap_old_ius_to_new_ius_in_mapping():
    remapping = {"A": ["A1", "A2"], "B": ["B"]}
    file = CustomFileInfo(
        scenario="s",
        scenario_index=0,
        total_scenarios=1,
        country="A",
        iu="A",
        file_path="A.csv",
    )

    result = remap_old_ius_to_new_ius(file, remapping)
    assert result == [
        CustomFileInfo(
            scenario="s",
            scenario_index=0,
            total_scenarios=1,
            country="A",
            iu="A1",
            file_path="A.csv",
        ),
        CustomFileInfo(
            scenario="s",
            scenario_index=0,
            total_scenarios=1,
            country="A",
            iu="A2",
            file_path="A.csv",
        ),
    ]


def test_remap_old_ius_to_new_ius_not_in_mapping():
    remapping = {"B": ["B"]}
    file = CustomFileInfo(
        scenario="s",
        scenario_index=0,
        total_scenarios=1,
        country="A",
        iu="A",
        file_path="A.csv",
    )

    result = remap_old_ius_to_new_ius(file, remapping)
    assert result == [
        CustomFileInfo(
            scenario="s",
            scenario_index=0,
            total_scenarios=1,
            country="A",
            iu="A",
            file_path="A.csv",
        ),
    ]
