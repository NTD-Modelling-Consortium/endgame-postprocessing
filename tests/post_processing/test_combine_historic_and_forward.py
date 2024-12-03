import os
from warnings import WarningMessage
import pandas as pd
import pandas.testing as pdt
from pyfakefs.fake_filesystem import FakeFilesystem
import pytest

from endgame_postprocessing.post_processing import combine_historic_and_forward


def test_combine_historic_and_forward(fs: FakeFilesystem):
    historic_canonical = pd.DataFrame(
        {
            "year": [2020],
            "scenario": ["scenario_0"],
            "draw_0": [0.1],
            "draw_1": [0.2],
        }
    )
    forward_canonical = pd.DataFrame(
        {
            "year": [2021],
            "scenario": ["scenario_1"],
            "draw_0": [0.2],
            "draw_1": [0.3],
        }
    )
    fs.create_file(
        "historic/AAA12345_scenario_0_canonical.csv",
        contents=historic_canonical.to_csv(index=False),
    )
    fs.create_file(
        "forward/AAA12345_scenario_1_canonical.csv",
        contents=forward_canonical.to_csv(index=False),
    )

    combine_historic_and_forward.combine_historic_and_forward(
        "historic", "forward", "output"
    )

    assert os.path.exists(
        "output/canonical_results/scenario_1/AAA/AAA12345/AAA12345_scenario_1_canonical.csv"
    )
    output_canonical = pd.read_csv(
        "output/canonical_results/scenario_1/AAA/AAA12345/AAA12345_scenario_1_canonical.csv"
    )
    pdt.assert_frame_equal(
        output_canonical,
        pd.DataFrame(
            {
                "year": [2020, 2021],
                "scenario": ["scenario_1"] * 2,
                "draw_0": [0.1, 0.2],
                "draw_1": [0.2, 0.3],
            }
        ),
    )


def test_combine_historic_and_forward_missing_historic_raises_error(fs: FakeFilesystem):
    historic_canonical = pd.DataFrame(
        {
            "year": [2020],
            "scenario": ["scenario_0"],
            "draw_0": [0.1],
            "draw_1": [0.2],
        }
    )
    forward_canonical = pd.DataFrame(
        {
            "year": [2021],
            "scenario": ["scenario_1"],
            "draw_0": [0.2],
            "draw_1": [0.3],
        }
    )
    fs.create_file(
        "historic/BBB12345_scenario_0_canonical.csv",
        contents=historic_canonical.to_csv(index=False),
    )
    fs.create_file(
        "forward/AAA12345_scenario_1_canonical.csv",
        contents=forward_canonical.to_csv(index=False),
    )
    with pytest.warns(
        combine_historic_and_forward.MissingHistoricDataException,
        match="Missing IU: AAA12345 in historic data",
    ) as w:
        combine_historic_and_forward.combine_historic_and_forward(
            "historic", "forward", "output"
        )


def test_combine_historic_and_forward_non_canonical_file(fs: FakeFilesystem):
    historic_canonical = pd.DataFrame(
        {
            "YEAR_ID": [2020],
            "scenario": ["scenario_0"],
            "draw_0": [0.1],
            "draw_1": [0.2],
        }
    )
    forward_canonical = pd.DataFrame(
        {
            "year": [2021],
            "scenario": ["scenario_1"],
            "draw_0": [0.2],
            "draw_1": [0.3],
        }
    )
    fs.create_file(
        "historic/AAA12345_scenario_0_canonical.csv",
        contents=historic_canonical.to_csv(index=False),
    )
    fs.create_file(
        "forward/AAA12345_scenario_1_canonical.csv",
        contents=forward_canonical.to_csv(index=False),
    )
    with pytest.warns(
        combine_historic_and_forward.MismatchedColumnsException,
        match="AAA12345 different columns in historic and forward projection",
    ):
        combine_historic_and_forward.combine_historic_and_forward(
            "historic", "forward", "output"
        )
