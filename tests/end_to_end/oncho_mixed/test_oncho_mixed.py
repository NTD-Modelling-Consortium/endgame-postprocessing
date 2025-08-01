import subprocess
from pathlib import Path
from tests.end_to_end.snapshot_with_csv import validate_expected_dir

def test_oncho_mix_end_to_end(snapshot):
    test_root = Path(__file__).parent
    main_folder_path = test_root.parent.parent.parent
    input_path = test_root / "example_input_data"
    output_path = Path(__file__).parent / "generated_data"
    known_good_subpath = "known_good_output"
    yaml_path = test_root / "scenario_1.yaml"
    mix_match_run_path = str(main_folder_path / "misc/pp_mixed_scenarios/post_process_mixed_scenarios.py")

    subprocess.run([
        "python",
        mix_match_run_path,
        "-w",
        str(input_path),
        "-o",
        str(output_path),
        "-s",
        str(yaml_path)
    ], check=True)

    validate_expected_dir(snapshot, test_root, output_path, known_good_subpath)
