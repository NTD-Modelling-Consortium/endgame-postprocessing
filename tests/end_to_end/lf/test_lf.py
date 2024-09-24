import shutil
import endgame_postprocessing.model_wrappers.lf.testRun as lf_runner

from pathlib import Path

from tests.end_to_end.generate_snapshot_dictionary import generate_snapshot_dictionary


def test_lf_end_to_end(snapshot):
    input_data = Path(__file__).parent / "example_input_data"
    output_path = Path(__file__).parent / "generated_data"

    if output_path.exists:
        shutil.rmtree(output_path)

    lf_runner.run_postprocessing_pipeline(
        input_dir=input_data, output_dir=output_path, num_jobs=1
    )
    snapshot.snapshot_dir = Path(__file__).parent

    results = generate_snapshot_dictionary(output_path)

    snapshot.assert_match_dir(results, "known_good_output")
