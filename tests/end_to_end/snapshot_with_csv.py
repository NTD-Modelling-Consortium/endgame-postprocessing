from pathlib import Path
import shutil
import sys

import pandas as pd
import pandas.testing as pdt
from tests.end_to_end.generate_snapshot_dictionary import generate_flat_snapshot_set, generate_snapshot_dictionary # noqa: E501


def validate_expected_dir(snapshot, test_root, output_path, known_good_subpath):
        # # Composite data is not part of the interface so don't check
    composite_path = output_path / "composite"
    shutil.rmtree(composite_path)

    results = sorted(generate_flat_snapshot_set(output_path))
    expected_results = sorted(
        generate_flat_snapshot_set(test_root / known_good_subpath)
    )

    snapshot.snapshot_dir = test_root

    if results != expected_results:
        snapshot.assert_match_dir(
            generate_snapshot_dictionary(output_path), known_good_subpath
        )
        # If updating we want to stop here
        return

    for actual_file_path, expected_file_path in zip(results, expected_results):
        assert actual_file_path == expected_file_path
        full_actual_path = output_path / actual_file_path
        full_expected_file_path = test_root / known_good_subpath / expected_file_path

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
                snapshot.assert_match_dir(
                    generate_snapshot_dictionary(output_path), known_good_subpath
                )
        else:
            actual_file_contents = Path(full_actual_path).read_text()
            expected_file_contents = Path(full_expected_file_path).read_text()
            try:
                assert actual_file_contents == expected_file_contents
            except AssertionError as mismatch_error:
                print(f"Mismatch in file {actual_file_path}:", file=sys.stderr)
                print(mismatch_error, file=sys.stderr)
                snapshot.assert_match_dir(
                    generate_snapshot_dictionary(output_path), "known_good_output"
                )
