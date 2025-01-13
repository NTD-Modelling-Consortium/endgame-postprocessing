import shutil


from pathlib import Path

from endgame_postprocessing.model_wrappers.sch import run_sch
from tests.end_to_end.snapshot_with_csv import validate_expected_dir

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

    validate_expected_dir(snapshot, test_root, output_path, known_good_subpath)


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

    validate_expected_dir(snapshot, test_root, output_path, known_good_subpath)
