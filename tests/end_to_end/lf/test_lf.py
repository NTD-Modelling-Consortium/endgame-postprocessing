import shutil

import pytest
import endgame_postprocessing.model_wrappers.lf.testRun as lf_runner

from pathlib import Path

from tests.end_to_end.snapshot_with_csv import validate_expected_dir


@pytest.mark.parametrize(
    "data_dir,scenario_with_historic_data",
    [("data_no_historic", None), ("data_with_historic", "scenario_0")],
)
def test_lf_end_to_end_no_historic(snapshot, data_dir, scenario_with_historic_data):
    test_root = Path(__file__).parent / data_dir
    input_data = test_root / "example_input_data"
    output_path = test_root / "generated_data"
    known_good_subpath = "known_good_output"

    if output_path.exists():
        shutil.rmtree(output_path)

    lf_runner.run_postprocessing_pipeline(
        forward_projection_raw=input_data,
        scenario_with_historic_data=scenario_with_historic_data,
        output_dir=output_path,
        num_jobs=1,
    )

    validate_expected_dir(snapshot, test_root, output_path, known_good_subpath)
