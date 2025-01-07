import shutil
import sys
import endgame_postprocessing.model_wrappers.oncho.testRun as oncho_runner

from pathlib import Path
import pandas as pd
import pandas.testing as pdt
import pytest

from tests.end_to_end.generate_snapshot_dictionary import (
    generate_flat_snapshot_set,
    generate_snapshot_dictionary,
)

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

def test_oncho_end_to_end(snapshot):
    input_data = Path(__file__).parent / "example_input_data" / "oncho"
    output_path = Path(__file__).parent / "generated_data"
    historic_data = Path(__file__).parent / "example_input_data" / "historic-oncho"

    if output_path.exists():
        shutil.rmtree(output_path)

    oncho_runner.run_postprocessing_pipeline(
        input_dir=input_data, output_dir=output_path, historic_dir=historic_data,
        historic_prefix="output_full_MTP_"
    )

    # Composite data is not part of the interface so don't check
    composite_path = output_path / "composite"
    shutil.rmtree(composite_path)

    results = sorted(generate_flat_snapshot_set(output_path))
    expected_results = sorted(
        generate_flat_snapshot_set(Path(__file__).parent / "known_good_output")
    )

    snapshot.snapshot_dir = Path(__file__).parent

    if results != expected_results:
        snapshot.assert_match_dir(
            generate_snapshot_dictionary(output_path), "known_good_output"
        )
        # If updating we want to stop here
        return

    for actual_file_path, expected_file_path in zip(results, expected_results):
        assert actual_file_path == expected_file_path
        full_actual_path = output_path / actual_file_path
        full_expected_file_path = (
            Path(__file__).parent / "known_good_output" / expected_file_path
        )

        actual_csv = pd.read_csv(full_actual_path)
        expected_csv = pd.read_csv(full_expected_file_path)

        try:
            pdt.assert_frame_equal(
                actual_csv,
                expected_csv,
            )
        except AssertionError as pandas_error:
            print(f"Mismatch in file {actual_file_path}:", file=sys.stderr)
            print(pandas_error, file=sys.stderr)
            snapshot.assert_match_dir(
                generate_snapshot_dictionary(output_path), "known_good_output"
            )
