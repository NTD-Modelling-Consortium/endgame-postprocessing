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
    with pytest.raises(Exception):
        _ = [f for f in file_util.post_process_file_generator("input-data")]
