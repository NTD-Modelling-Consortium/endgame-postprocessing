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
def test_get_standard_name_for_historic_sth_file(historic_name, new_name):
    assert new_name == rename_historic.get_standard_name_for_historic_sth_file(
        historic_name
    )


@pytest.mark.parametrize(
    "historic_name,new_name",
    [
        (
            "PrevDataset_Haema_BBB00001.csv",
            "ntdmc-BBB00001-haematobium-group_001-scenario_0-survey_type_kk2-group_001-200_simulations.csv",
        ),
        (
            "PrevDataset_Man_Low_BBB00001.csv",
            "ntdmc-BBB00001-mansoni_low_burden-group_001-scenario_0-survey_type_kk2-group_001-200_simulations.csv",
        ),
        (
            "PrevDataset_Man_High_BBB00001.csv",
            "ntdmc-BBB00001-mansoni_high_burden-group_001-scenario_0-survey_type_kk2-group_001-200_simulations.csv",
        ),
    ],
)
def test_get_standard_name_for_historic_sch_file(historic_name, new_name):
    assert new_name == rename_historic.get_standard_name_for_historic_sch_file(
        historic_name
    )


def test_rename_flat_historic_data_sth(fs):
    fs.create_file("foo/PrevDataset_Hook_AAA00001.csv"),
    rename_historic.rename_historic_sth_files("foo", "bar")
    assert os.path.exists(
        "bar/ntdmc-AAA00001-hookworm-group_001-scenario_0-group_001-200_simulations.csv"
    )
    assert os.path.exists("foo/PrevDataset_Hook_AAA00001.csv")


def test_rename_flat_historic_data_sch(fs):
    fs.create_file("foo/PrevDataset_Haema_AAA00001.csv"),
    rename_historic.rename_historic_sch_files("foo", "bar")
    assert os.path.exists(
        "bar/ntdmc-AAA00001-haematobium-group_001-scenario_0-survey_type_kk2-group_001-200_simulations.csv"
    )
    assert os.path.exists("foo/PrevDataset_Haema_AAA00001.csv")
