import shutil
import endgame_postprocessing.model_wrappers.oncho.testRun as oncho_runner

from pathlib import Path
import pytest
import json

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

    oncho_runner.run_postprocessing_pipeline(
        input_dir=input_data, output_dir=output_path, historic_dir=historic_data,
        historic_prefix="output_full_MTP_",
        start_year=2000
    )

    with open(f'{output_path}/aggregation_info.json', "r") as f:
        aggregation_info = json.load(f)

        # TODO: fix the warning/test so that it is consistant between dev and prod. Currently,
        # the warning outputs the full directory, which will be different in github and locally
        new_warnings = [
            warning
            for warning in aggregation_info["warnings"]
            if "example_input_data/oncho/PopulationMetadatafile.csv"
            not in warning["message"]
        ]
    with open(f'{output_path}/aggregation_info.json', "w") as f:
        new_aggregation_info = aggregation_info
        new_aggregation_info["warnings"] = new_warnings
        json.dump(new_aggregation_info, f, indent=4)

    validate_expected_dir(snapshot, test_root, output_path, known_good_subpath)
