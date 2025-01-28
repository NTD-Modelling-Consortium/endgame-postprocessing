import warnings

import pytest

import endgame_postprocessing.post_processing.file_util as file_util
from endgame_postprocessing.post_processing.custom_file_info import CustomFileInfo


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


def test_wrong_kind_of_file_in_iu_directory(fs):
    fs.create_file("input-data/scenario1/country/iu1/extra_file.txt")
    fs.create_file("input-data/scenario1/country/iu1/data.csv")
    with warnings.catch_warnings(record=True) as w:
        _ = [f for f in file_util.post_process_file_generator("input-data")]
        assert [str(warning.message) for warning in w] == [
            "Unexpected file extra_file.txt in IUs directory input-data/scenario1/country/iu1, "
            "expecting .csv only"
        ]


def test_directory_in_iu_raises_warning(fs):
    fs.create_dir("input-data/scenario1/country/iu1/extra_directory/")
    fs.create_file("input-data/scenario1/country/iu1/data.csv")
    with warnings.catch_warnings(record=True) as w:
        _ = [f for f in file_util.post_process_file_generator("input-data")]
        assert [str(warning.message) for warning in w] == [
            "1 unexpected subdirectories in IU directory input-data/scenario1/country/iu1, "
            "contents will be ignored"
        ]


def test_no_csvs_in_iu_dir_raises_warning(fs):
    fs.create_dir("input-data/scenario1/country/iu1/")
    with warnings.catch_warnings(record=True) as w:
        _ = [f for f in file_util.post_process_file_generator("input-data")]
        assert [str(warning.message) for warning in w] == [
            "No IU data files found for IU input-data/scenario1/country/iu1"
        ]


def test_write_to_csv_success(fs):
    fs.create_dir("input-data/historic_disease/")
    test_file = "input-data/historic_disease/random_test_AAA0000012345.csv"
    fs.create_file(test_file)
    match = file_util.get_matching_csv(
        "input-data/historic_disease/",
        "random_test_",
        country_code="AAA",
        iu_number="12345",
        scenario="test_scenario",
    )
    assert match == test_file


def test_write_to_csv_warning_no_file_match(fs):
    fs.create_dir("input-data/historic_disease/")
    test_file = "input-data/historic_disease/random_test_AAA0000012345.csv"
    fs.create_file(test_file)
    with warnings.catch_warnings(record=True) as w:
        match = file_util.get_matching_csv(
            "input-data/historic_disease/",
            "random_test_",
            country_code="AAA",
            iu_number="12346",
            scenario="test_scenario",
        )
        assert [str(warning.message) for warning in w] == [
            "IU AAA12346 found in test_scenario but not found in histories."
        ]
        assert match is None


def test_write_to_csv_exception_multiple_matches(fs):
    fs.create_dir("input-data/historic_disease/")
    test_file = "input-data/historic_disease/random_test_AAA0000012346.csv"
    test_file = "input-data/historic_disease/random_test_AAA0001012346.csv"
    fs.create_file(test_file)
    with pytest.raises(Exception) as e:
        file_util.get_matching_csv(
            "input-data/historic_disease/",
            "random_test_",
            country_code="AAA",
            iu_number="12346",
            scenario="test_scenario",
        )
        str(e.value) == "Expected exactly one file for random_test_AAA12345, found 2"


def test_list_all_historic_ius_long_iu_numbers(fs):
    fs.create_dir("historic_disease/")
    fs.create_file("historic_disease/random_prefix_AAA0000012345.csv")
    fs.create_file("historic_disease/random_prefix_AAA0000012346.csv")
    matches = set(
        file_util.list_all_historic_ius("historic_disease/", "random_prefix_").keys()
    )
    assert matches == set(["AAA12345", "AAA12346"])


def test_list_all_historic_ius_short_iu_numbers(fs):
    fs.create_dir("historic_disease/")
    fs.create_file("historic_disease/random_prefix_AAA12345.csv")
    fs.create_file("historic_disease/random_prefix_AAA12346.csv")
    matches = set(
        file_util.list_all_historic_ius("historic_disease/", "random_prefix_").keys()
    )
    assert matches == set(["AAA12345", "AAA12346"])
