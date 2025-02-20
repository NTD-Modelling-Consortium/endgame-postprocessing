import argparse
import json
import os
import re
import shutil
import time
from concurrent.futures import ThreadPoolExecutor, wait
from pathlib import Path
from pprint import pprint
from typing import List, Tuple, Callable, Optional

import pandas as pd
import yaml

from endgame_postprocessing.post_processing import pipeline, output_directory_structure
from endgame_postprocessing.post_processing.disease import Disease
from endgame_postprocessing.post_processing.generation_metadata import produce_generation_metadata
from endgame_postprocessing.post_processing.pipeline_config import PipelineConfig


def _validate_working_directory(working_directory):
    """
    Validate the structure of the working directory to ensure required files and folders exist.
    Raises FileNotFoundError if a required file or directory is missing.

    :param working_directory: Path to the working directory.
    """
    input_directory = working_directory / "input"
    if not (input_directory / "PopulationMetadatafile.csv").exists():
        raise FileNotFoundError(
            f"Required PopulationMetadatafile.csv not found. Please ensure it exists in the input directory."
        )
    if not (input_directory / "canonical_results").is_dir():
        raise FileNotFoundError(
            f"Required directory 'canonical_results' not found in the input directory. "
            f"Please ensure it exists and contains the necessary scenario subdirectories."
        )
    return input_directory / "canonical_results"


def _load_mixed_scenarios_desc(working_directory):
    mixed_scenarios_desc_file = working_directory / "mixed_scenarios_desc.yaml"
    if not mixed_scenarios_desc_file.exists():
        raise FileNotFoundError(
            f"Required file '{mixed_scenarios_desc_file}' not found. Please ensure it exists in the working directory."
        )
    mixed_scenarios_desc = yaml.load(mixed_scenarios_desc_file.read_text(), Loader=yaml.FullLoader)
    return mixed_scenarios_desc


def _get_pipeline_config_from_scenario_file(mixed_scenarios_desc):
    if mixed_scenarios_desc["disease"] == "oncho":
        disease = Disease.ONCHO
    elif mixed_scenarios_desc["disease"] == "trachoma":
        disease = Disease.TRACHOMA
    else:
        disease = Disease.LF

    if "threshold" in mixed_scenarios_desc:
        threshold = float(mixed_scenarios_desc["threshold"])
        return PipelineConfig(disease=disease, threshold=threshold)
    else:
        return PipelineConfig(disease=disease)


def _collect_source_target_paths(
    input_canonical_results_dir: Path,
    output_scenario_directory: Path,
    mixed_scenarios_desc: dict,
) -> List[Tuple[Path, Path]]:
    """
    Collect all source-to-destination mappings in one pass for the default scenario and overridden IUs.

    :param input_canonical_results_dir: Path to the base canonical_results directory.
    :param output_scenario_directory: Path to the output scenario directory.
    :param mixed_scenarios_desc: Dictionary describing the mixed scenarios.
    :return: List of tuples containing (source_path, destination_path).
    """
    paths_to_copy = []

    # Add default scenario directory
    default_scenario_source = input_canonical_results_dir / mixed_scenarios_desc["default_scenario"]
    paths_to_copy.append((default_scenario_source, output_scenario_directory))

    # Add overridden IU directories
    for scenario, ius in mixed_scenarios_desc["overridden_ius"].items():
        input_scenario_directory = input_canonical_results_dir / scenario
        for iu in ius:
            source_path = input_scenario_directory / iu[:3] / iu
            destination_path = output_scenario_directory / iu[:3] / iu
            paths_to_copy.append((source_path, destination_path))

    return paths_to_copy


def _copy_with_rename(
    paths_to_copy: List[Tuple[Path, Path]],
    pattern: str,
    replacement: str,
    cb: Optional[Callable[[Path], None]] = None,
):
    """
    Perform a single pass over the collected paths and copy files with renaming applied.

    :param paths_to_copy: List of tuples containing (source_path, destination_path).
    :param pattern: Regex pattern to match in filenames.
    :param replacement: Replacement string for the matched part of filenames.
    :param cb: Optional callback function to execute after each file copy, on the copied file.
    """

    def worker(src: Path, dst: Path):
        """
        Worker function to copy files or directories with renaming applied to files.
        """
        if src.is_dir():
            for item in src.rglob("*"):  # Recursively copy all contents
                relative_path = item.relative_to(src)
                target_path = dst / relative_path

                if item.is_dir():
                    target_path.mkdir(exist_ok=True)
                else:  # Apply renaming logic to files
                    renamed_target_path = target_path.with_name(
                        re.sub(pattern, replacement, target_path.name)
                    )
                    shutil.copy2(item, renamed_target_path)

                    if cb:
                        cb(renamed_target_path)
        else:
            # If src is a single file (unlikely in this case), copy and rename directly
            renamed_target_path = dst.with_name(re.sub(pattern, replacement, dst.name))
            shutil.copy2(src, renamed_target_path)

    # Use ThreadPoolExecutor for parallel execution
    with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
        # Submit tasks for each source-destination pair
        futures = [executor.submit(worker, src, dst) for src, dst in paths_to_copy]
        wait(futures)


def _prepare_output_directory(working_directory, mixed_scenarios_desc, pattern, replacement):
    """
    Prepare the output directory structure, copy files, and rename them during copying.

    :param working_directory: Path to the working directory (Path object).
    :param mixed_scenarios_desc: Dictionary with mixed scenarios description.
    :param pattern: Regex pattern to match in filenames.
    :param replacement: String to replace the matched portion of filenames.
    :return: Path to the prepared output canonical results directory.
    """
    input_canonical_results_dir = working_directory / "input" / "canonical_results"
    output_canonical_results_dir = working_directory / "output" / "canonical_results"
    output_canonical_results_dir.mkdir(parents=True, exist_ok=True)

    output_scenario_directory = output_canonical_results_dir / mixed_scenarios_desc["scenario_name"]
    output_scenario_directory.mkdir(parents=True, exist_ok=True)

    # Collect all source and target paths
    paths_to_copy = _collect_source_target_paths(
        input_canonical_results_dir, output_scenario_directory, mixed_scenarios_desc
    )

    def rename_scenario_column(path_to_iu: Path):
        df = pd.read_csv(path_to_iu)
        df["scenario"] = mixed_scenarios_desc["scenario_name"]
        df.to_csv(path_to_iu, index=False)

    # Perform the copy and renaming in a single pass
    _copy_with_rename(
        paths_to_copy,
        pattern,
        replacement,
        rename_scenario_column,
    )

    # Write mixed_scenarios_desc to a JSON file in the output directory
    mixed_scenarios_metadata_path = (
        output_canonical_results_dir.parent / "mixed_scenarios_metadata.json"
    )
    mixed_scenarios_metadata_path.write_text(json.dumps(mixed_scenarios_desc, indent=4))

    return output_canonical_results_dir


def main():
    # For the script to work,
    # we need to make sure the user has set up a working directory
    # containing at least the directories and files marked "user provided"
    #
    # working_directory
    #   - mixed_scenarios_desc.yaml (user provided)
    #   - input_directory (user provided)
    #      - canonical_results
    #         - scenario_0
    #         - scenario_1
    #         ...
    #      - PopulationMetadatafile.csv (user provided)
    #   - output_directory (created by the script)
    #      - canonical_results
    #         - scenario_x1 (name specified in `mixed_scenarios_desc.yaml`
    #      - mixed_scenarios_metadata.json

    parser = argparse.ArgumentParser(description="Postprocess mixed scenarios.")
    parser.add_argument(
        "-w",
        "--working-directory",
        type=str,
        required=True,
        help="Path to the working directory. See README for more details.",
    )
    args = parser.parse_args()

    working_directory = Path(args.working_directory).resolve()
    print(f"Working directory: {working_directory}")

    t_start = time.time()
    try:
        input_canonical_results_dir = _validate_working_directory(working_directory)
    except FileNotFoundError as e:
        print(f"Error: {e}")
        return

    try:
        mixed_scenarios_desc = _load_mixed_scenarios_desc(working_directory)
        print("The mixed scenarios description file has been successfully loaded:")
        pprint(mixed_scenarios_desc, indent=2)
    except FileNotFoundError as e:
        print(f"Error: {e}")
        return

    output_canonical_results_dir = _prepare_output_directory(
        working_directory,
        mixed_scenarios_desc,
        r"scenario_\w+_",
        f"{mixed_scenarios_desc['scenario_name']}_",
    )
    t_finish = time.time()
    print(f"Time taken to prepare output directory: {t_finish - t_start:.2f} seconds")

    t_start = time.time()

    pipeline_config = _get_pipeline_config_from_scenario_file(mixed_scenarios_desc)

    pipeline.pipeline(
        input_canonical_results_dir.parent,
        output_canonical_results_dir.parent,
        pipeline_config,
    )

    output_directory_structure.write_results_metadata_file(
        output_canonical_results_dir.parent, produce_generation_metadata(warnings=[])
    )

    t_finish = time.time()
    print(f"Time taken to run pipeline: {t_finish - t_start:.2f} seconds")


if __name__ == "__main__":
    main()
