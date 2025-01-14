import os
import endgame_postprocessing.model_wrappers.sch.rename_historic as rename_historic

def test_rename_flat_historic_data(fs):
    fs.create_file(
        "foo/PrevDataset_Hook_AAA00001.csv"),
    rename_historic.rename_historic_files("foo", "bar")
    assert os.path.exists("bar/ntdmc-AAA00001-hookworm-group_001-scenario_0_survey_type_kk2-group_001-200_simulations.csv")
    assert os.path.exists("foo/PrevDataset_Hook_AAA00001.csv")
