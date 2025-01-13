import shutil
import warnings
import endgame_postprocessing.model_wrappers.oncho.testRun as oncho_runner

from pathlib import Path
import pytest

from tests.end_to_end.snapshot_with_csv import validate_expected_dir

def test_oncho_empty_input_directory(mocker):
    with pytest.raises(Exception) as exception:
        file_gen_mock = mocker.patch(
            "endgame_postprocessing.model_wrappers.oncho.testRun.post_process_file_generator"
        )
        file_gen_mock.return_value = []
        oncho_runner.run_postprocessing_pipeline(
            input_dir=Path(__file__).parent / "empty_input_data",
            output_dir=Path(__file__).parent / "generated_data"
        )

    assert "No data for IUs found - see above warnings and check input directory" in str(exception)

def test_oncho_bad_historic_prefix():
    input_data = Path(__file__).parent / "example_input_data" / "oncho"
    output_path = Path(__file__).parent / "generated_data"
    historic_data = Path(__file__).parent / "example_input_data" / "historic-oncho"

    if output_path.exists():
        shutil.rmtree(output_path)

    with pytest.raises(Exception):
        oncho_runner.run_postprocessing_pipeline(
            input_dir=input_data, output_dir=output_path, historic_dir=historic_data,
            historic_prefix="*"
        )

def test_oncho_end_to_end(snapshot):
    test_root = Path(__file__).parent
    input_data = test_root / "example_input_data" / "oncho"
    output_path = test_root / "generated_data"
    historic_data = test_root / "example_input_data" / "historic-oncho"
    known_good_subpath = "known_good_output"

    if output_path.exists():
        shutil.rmtree(output_path)

    with warnings.catch_warnings(record=True) as w:
        oncho_runner.run_postprocessing_pipeline(
            input_dir=input_data, output_dir=output_path, historic_dir=historic_data,
            historic_prefix="output_full_MTP_"
        )
        assert (
            "IU AAA00007 found in scenario_1 but not found in histories." in
            [str(warning.message) for warning in w]
        )
        assert (
            "IU AAA00007 found in scenario_2 but not found in histories." in
            [str(warning.message) for warning in w]
        )
        assert (
            "IU AAA00005 was not found in forward_projections and as such will not have the historic data" in # noqa 501
            [str(warning.message) for warning in w]
        )

    validate_expected_dir(snapshot, test_root, output_path, known_good_subpath)
