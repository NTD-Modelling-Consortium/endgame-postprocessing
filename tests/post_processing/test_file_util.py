import warnings
import pytest
from endgame_postprocessing.post_processing.dataclasses import CustomFileInfo
import endgame_postprocessing.post_processing.file_util as file_util


def test_post_process_file_generator_with_correct_structure(fs):
    fs.create_file("input-data/scenario1/country/iu1/data.csv")
    fs.create_file("input-data/scenario1/country/iu2/data.csv")
    fs.create_file("input-data/scenario1/country2/iu3/data.csv")
    fs.create_file("input-data/scenario2/country2/iu3/data.csv")

    results = [f for f in file_util.post_process_file_generator("input-data")]
    assert results == [
        CustomFileInfo(
            0,
            2,
            "scenario1",
            "country",
            "iu1",
            "input-data/scenario1/country/iu1/data.csv",
        ),
        CustomFileInfo(
            0,
            2,
            "scenario1",
            "country",
            "iu2",
            "input-data/scenario1/country/iu2/data.csv",
        ),
        CustomFileInfo(
            0,
            2,
            "scenario1",
            "country2",
            "iu3",
            "input-data/scenario1/country2/iu3/data.csv",
        ),
        CustomFileInfo(
            1,
            2,
            "scenario2",
            "country2",
            "iu3",
            "input-data/scenario2/country2/iu3/data.csv",
        ),
    ]


def test_post_process_file_generator_with_extra_file_in_scenario_dir(fs):
    fs.create_file("input-data/extra_file.txt")
    fs.create_file("input-data/scenario1/country/iu1/data.csv")
    results = [f for f in file_util.post_process_file_generator("input-data")]
    assert results == [
        CustomFileInfo(
            0,
            1,
            "scenario1",
            "country",
            "iu1",
            "input-data/scenario1/country/iu1/data.csv",
        )
    ]


def test_empty_directory_results_in_exception(fs):
    fs.create_dir("input-data")
    with pytest.raises(Exception) as raised_exception:
        _ = [f for f in file_util.post_process_file_generator("input-data")]
    assert str(raised_exception.value) == "No scenario directories found in input-data"


def test_file_in_scenario_dir_raises_warning(fs):
    fs.create_file("input-data/extra_file.txt")
    fs.create_file("input-data/scenario1/country/iu1/data.csv")
    with warnings.catch_warnings(record=True) as w:
        _ = [f for f in file_util.post_process_file_generator("input-data")]
        assert [str(warning.message) for warning in w] == [
            "Unexpected file input-data/extra_file.txt found in input-data"
        ]
