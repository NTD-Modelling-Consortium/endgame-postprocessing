import shutil
import sys


from pathlib import Path
import pandas as pd
import pandas.testing as pdt

from endgame_postprocessing.model_wrappers.sch import run_sch
from tests.end_to_end.generate_snapshot_dictionary import (
    generate_flat_snapshot_set,
    generate_snapshot_dictionary,
)

def _validate_expected_dir(snapshot, test_root, output_path, known_good_subpath):
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


def test_sth_end_to_end_no_historic_one_worm(snapshot):
    data_dir = "data_no_historic"
    test_root = Path(__file__).parent / data_dir
    input_data = test_root / "example_input_data"
    output_path = test_root / "generated_data"
    known_good_subpath = "known_good_output"

    if output_path.exists():
        shutil.rmtree(output_path)

    run_sch.run_sth_postprocessing_pipeline(
        input_data,
        output_dir=output_path,
        worm_directories=["ascaris"],
        num_jobs=1,
        run_country_level_summaries=True,
    )

    _validate_expected_dir(snapshot, test_root, output_path, known_good_subpath)


def test_sth_end_to_end_no_historic_many_worms(snapshot):
    data_dir = "data_no_historic"
    test_root = Path(__file__).parent / data_dir
    input_data = test_root / "example_input_data"
    output_path = test_root / "generated_data_combined_worms"
    known_good_subpath = "known_good_output_combined_worms"

    if output_path.exists():
        shutil.rmtree(output_path)

    run_sch.run_sth_postprocessing_pipeline(
        f"{input_data}/",  # currently requires trailing slash
        output_dir=output_path,
        worm_directories=[
            "hookworm",
            "ascaris",
        ],
        num_jobs=1,
        run_country_level_summaries=True,
    )

    _validate_expected_dir(snapshot, test_root, output_path, known_good_subpath)
