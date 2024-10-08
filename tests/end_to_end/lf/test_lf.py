import shutil
import sys
import endgame_postprocessing.model_wrappers.lf.testRun as lf_runner

from pathlib import Path
import pandas as pd
import pandas.testing as pdt

from tests.end_to_end.generate_snapshot_dictionary import (
    generate_flat_snapshot_set,
    generate_snapshot_dictionary,
)


def test_lf_end_to_end(snapshot):
    input_data = Path(__file__).parent / "example_input_data"
    output_path = Path(__file__).parent / "generated_data"

    if output_path.exists():
        shutil.rmtree(output_path)

    lf_runner.run_postprocessing_pipeline(
        input_dir=input_data, output_dir=output_path, num_jobs=1
    )

    results = sorted(generate_flat_snapshot_set(output_path))
    expected_results = sorted(
        generate_flat_snapshot_set(Path(__file__).parent / "known_good_output")
    )

    snapshot.snapshot_dir = Path(__file__).parent

    if results != expected_results:
        snapshot.assert_match_dir(
            generate_snapshot_dictionary(output_path), "known_good_output"
        )

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
