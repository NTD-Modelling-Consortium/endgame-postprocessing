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


def test_sch_end_to_end_no_historic_one_worm(snapshot):
    data_dir = "data_no_historic"
    test_root = Path(__file__).parent / data_dir
    input_data = test_root / "example_input_data"
    output_path = test_root / "generated_data"

    if output_path.exists():
        shutil.rmtree(output_path)

    run_sch.run_sch_postprocessing_pipeline(
        f"{input_data}/",  # currently requires trailing slash
        historic_input_dir=None,
        output_dir=output_path,
        worm_directories=["sch-haematobium"],
        run_country_level_summaries=True,
    )

    # # Composite data is not part of the interface so don't check
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


def test_sch_end_to_end_no_historic_many_worms(snapshot):
    data_dir = "data_no_historic"
    test_root = Path(__file__).parent / data_dir
    input_data = test_root / "example_input_data"
    output_path = test_root / "generated_data_combined_worms"

    if output_path.exists():
        shutil.rmtree(output_path)

    run_sch.run_sch_postprocessing_pipeline(
        f"{input_data}/",  # currently requires trailing slash
        historic_input_dir=None,
        output_dir=output_path,
        worm_directories=[
            "sch-haematobium",
            "sch-mansoni-high-burden",
            "sch-mansoni-low-burden",
        ],
        run_country_level_summaries=True,
    )

    # # Composite data is not part of the interface so don't check
    composite_path = output_path / "composite"
    shutil.rmtree(composite_path)

    results = sorted(generate_flat_snapshot_set(output_path))
    expected_results = sorted(
        generate_flat_snapshot_set(test_root / "known_good_output_combined_worms")
    )

    snapshot.snapshot_dir = test_root

    if results != expected_results:
        snapshot.assert_match_dir(
            generate_snapshot_dictionary(output_path),
            "known_good_output_combined_worms",
        )
        # If updating we want to stop here
        return

    for actual_file_path, expected_file_path in zip(results, expected_results):
        assert actual_file_path == expected_file_path
        full_actual_path = output_path / actual_file_path
        full_expected_file_path = (
            test_root / "known_good_output_combined_worms" / expected_file_path
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
                generate_snapshot_dictionary(output_path),
                "known_good_output_combined_worms",
            )
