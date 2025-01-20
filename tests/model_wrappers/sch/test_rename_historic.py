import os

import pytest
import endgame_postprocessing.model_wrappers.sch.rename_historic as rename_historic


@pytest.mark.parametrize(
    "historic_name,new_name",
    [
        (
            "PrevDataset_Hook_AAA00001.csv",
            "ntdmc-AAA00001-hookworm-group_001-scenario_0-group_001-200_simulations.csv",
        ),
        (
            "PrevDataset_Asc_AAA00002.csv",
            "ntdmc-AAA00002-ascaris-group_001-scenario_0-group_001-200_simulations.csv",
        ),
        (
            "PrevDataset_Tri_BBB00001.csv",
            "ntdmc-BBB00001-trichuris-group_001-scenario_0-group_001-200_simulations.csv",
        ),
    ],
)
def test_get_standard_name_for_historic_file(historic_name, new_name):
    assert new_name == rename_historic.get_standard_name_for_historic_sth_file(
        historic_name
    )


def test_rename_flat_historic_data(fs):
    fs.create_file("foo/PrevDataset_Hook_AAA00001.csv"),
    rename_historic.rename_historic_sth_files("foo", "bar")
    assert os.path.exists(
        "bar/ntdmc-AAA00001-hookworm-group_001-scenario_0_survey_type_kk2-group_001-200_simulations.csv"
    )
    assert os.path.exists("foo/PrevDataset_Hook_AAA00001.csv")
