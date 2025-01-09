import shutil
import sys

import pytest
import endgame_postprocessing.model_wrappers.lf.testRun as lf_runner

from pathlib import Path
import pandas as pd
import pandas.testing as pdt

from tests.end_to_end.generate_snapshot_dictionary import (
    generate_flat_snapshot_set,
    generate_snapshot_dictionary,
)


@pytest.mark.parametrize(
    "data_dir,scenario_with_historic_data",
    [("data_no_historic", None), ("data_with_historic", "scenario_0")],
)
def test_lf_end_to_end_no_historic(snapshot, data_dir, scenario_with_historic_data):
    test_root = Path(__file__).parent / data_dir
    input_data = test_root / "example_input_data"
    output_path = test_root / "generated_data"

    if output_path.exists():
        shutil.rmtree(output_path)

    lf_runner.run_postprocessing_pipeline(
        forward_projection_raw=input_data,
        scenario_with_historic_data=scenario_with_historic_data,
        output_dir=output_path,
        num_jobs=1,
    )

    # Composite data is not part of the interface so don't check
    composite_path = output_path / "composite"
    shutil.rmtree(composite_path)

    results = sorted(generate_flat_snapshot_set(output_path))
    expected_results = sorted(
        generate_flat_snapshot_set(test_root / "known_good_output")
    )

    snapshot.snapshot_dir = test_root

    if results != expected_results:
        snapshot.assert_match_dir(
            generate_snapshot_dictionary(output_path), "known_good_output"
        )
        # If updating we want to stop here
        return

    for actual_file_path, expected_file_path in zip(results, expected_results):
        assert actual_file_path == expected_file_path
        full_actual_path = output_path / actual_file_path
        full_expected_file_path = test_root / "known_good_output" / expected_file_path

        if actual_file_path.endswith(".csv"):
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
                break
    snapshot.assert_match_dir(
        generate_snapshot_dictionary(output_path), "known_good_output"
    )
