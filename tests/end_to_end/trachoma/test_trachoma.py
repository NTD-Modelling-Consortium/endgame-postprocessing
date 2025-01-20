import shutil
from pathlib import Path

import pytest

from endgame_postprocessing.model_wrappers.trachoma import run_trach
from tests.end_to_end.snapshot_with_csv import validate_expected_dir


def test_trachoma_empty_input_directory(mocker):
    with pytest.raises(
        Exception,
        match="No data for IUs found - see above warnings and check input directory",
    ):
        file_gen_mock = mocker.patch(
            "endgame_postprocessing.model_wrappers.trachoma.run_trach.file_util.get_flat_regex",
        )

        file_gen_mock.return_value = []
        run_trach.run_postprocessing_pipeline(
            input_dir=Path(__file__).parent / "empty_input_data",
            output_dir=Path(__file__).parent / "generated_data",
        )


def test_trachoma_end_to_end(snapshot):
    test_root = Path(__file__).parent
    input_data = test_root / "example_input_data" / "trachoma"
    output_path = test_root / "generated_data"
    known_good_subpath = "known_good_output"

    if output_path.exists():
        shutil.rmtree(output_path)

    run_trach.run_postprocessing_pipeline(
        input_dir=input_data,
        output_dir=output_path,
        start_year=2026,
    )

    validate_expected_dir(snapshot, test_root, output_path, known_good_subpath)


def test_trachoma_end_to_end_historic(snapshot):
    test_root = Path(__file__).parent
    input_data = test_root / "example_input_data" / "trachoma"
    output_path = test_root / "generated_data"
    historic_data = test_root / "example_input_data" / "historic-trachoma"
    known_good_subpath = "known_good_output_historic"

    if output_path.exists():
        shutil.rmtree(output_path)

    run_trach.run_postprocessing_pipeline(
        input_dir=input_data,
        output_dir=output_path,
        historic_dir=historic_data,
        historic_prefix="PrevDataset_Trachoma_",
        start_year=2000,
    )

    validate_expected_dir(snapshot, test_root, output_path, known_good_subpath)


def test_trachoma_end_to_end_historic_missing_iu(snapshot):
    test_root = Path(__file__).parent
    input_data = test_root / "example_input_data" / "trachoma"
    output_path = test_root / "generated_data"
    historic_data = test_root / "example_input_data" / "historic-trachoma_missing_iu"
    known_good_subpath = "known_good_output_historic_missing_iu"

    if output_path.exists():
        shutil.rmtree(output_path)

    run_trach.run_postprocessing_pipeline(
        input_dir=input_data,
        output_dir=output_path,
        historic_dir=historic_data,
        historic_prefix="PrevDataset_Trachoma_",
        start_year=2000,
    )

    validate_expected_dir(snapshot, test_root, output_path, known_good_subpath)
