import argparse
import json
import os
import re
import shutil
import time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, wait
from dataclasses import dataclass
from itertools import chain
from pathlib import Path
from pprint import pprint
from typing import List, Tuple, Callable, Optional, Dict

import pandas as pd
import yaml

from endgame_postprocessing.post_processing import pipeline, output_directory_structure
from endgame_postprocessing.post_processing.disease import Disease
from endgame_postprocessing.post_processing.generation_metadata import produce_generation_metadata
from endgame_postprocessing.post_processing.pipeline_config import PipelineConfig
from endgame_postprocessing.post_processing.warnings_collector import CollectAndPrintWarnings
from misc.pp_mixed_scenarios.exceptions import (
    MissingCanonicalResultsDirectoryError,
    MissingPopulationMetadataFileError,
    MissingScenariosFromSpecificationError,
    DuplicateIUError,
    MixedScenariosFileNotFound,
    MissingFieldsError,
    InvalidOverriddenIUsError,
    InvalidDiseaseFieldError,
    InvalidThresholdError,
)


@dataclass
class MixedScenariosDescription:
    disease: str
    threshold: Optional[float]
    default_scenario: Optional[str]
    overridden_ius: Dict[str, List[str]]
    scenario_name: str

    @staticmethod
    def from_dict(data: Dict) -> "MixedScenariosDescription":
        return MixedScenariosDescription(
            disease=data["disease"],
            threshold=data.get("threshold"),
            default_scenario=data.get("default_scenario"),
            overridden_ius=data["overridden_ius"],
            scenario_name=data["scenario_name"],
        )

    def to_dict(self) -> Dict:
        return {
            "disease": self.disease,
            "threshold": self.threshold,
            "default_scenario": self.default_scenario,
            "overridden_ius": self.overridden_ius,
            "scenario_name": self.scenario_name,
        }

    @staticmethod
    def from_yaml(yaml_str: str) -> "MixedScenariosDescription":
        data = yaml.safe_load(yaml_str)
        return MixedScenariosDescription.from_dict(data)

    def to_yaml(self) -> str:
        return yaml.dump(self.to_dict(), sort_keys=False)

    @staticmethod
    def from_json(json_str: str) -> "MixedScenariosDescription":
        data = json.loads(json_str)
        return MixedScenariosDescription.from_dict(data)

    def to_json(self, indent: int = 4) -> str:
        return json.dumps(self.to_dict(), indent=indent)


def _validate_working_directory(
    working_directory: Path, mixed_scenarios_desc: MixedScenariosDescription
):
    """
    Validate the structure of the working directory to ensure required files and folders exist.
    Raises FileNotFoundError if a required file or directory is missing.

    :param working_directory: Path to the working directory.
    """
    input_directory = working_directory / "input"
    population_metadata_file = input_directory / "PopulationMetadatafile.csv"
    if not population_metadata_file.exists():
        raise MissingPopulationMetadataFileError(population_metadata_file)

    input_canonical_results_dir = input_directory / "canonical_results"
    if not input_canonical_results_dir.is_dir():
        raise MissingCanonicalResultsDirectoryError(input_canonical_results_dir)

    # We'll check that the scenarios listed as `default` (if it exists) and `overridden`
    # in the mixed scenarios description yaml are available in the source data
    overridden_scenarios = mixed_scenarios_desc.overridden_ius.keys()
    listed_scenarios = (
        {mixed_scenarios_desc.default_scenario, *overridden_scenarios}
        if mixed_scenarios_desc.default_scenario
        else overridden_scenarios
    )
    available_directories = {d.name for d in input_canonical_results_dir.iterdir() if d.is_dir()}

    missing_scenarios = listed_scenarios - available_directories
    if missing_scenarios:
        raise MissingScenariosFromSpecificationError(missing_scenarios)

    return input_directory


def _find_duplicate_ius(overridden_ius_dict: Dict[str, List[str]]):
    iu_counter = Counter(chain.from_iterable(overridden_ius_dict.values()))

    duplicated_iu_scenarios = defaultdict(set)
    for scenario, ius in overridden_ius_dict.items():
        for iu in ius:
            if iu_counter[iu] > 1:
                duplicated_iu_scenarios[iu].add(scenario)

    if duplicated_iu_scenarios:
        raise DuplicateIUError(duplicated_iu_scenarios)


def _load_mixed_scenarios_desc(mixed_scenarios_desc_file: Path) -> MixedScenariosDescription:
    if not mixed_scenarios_desc_file.exists():
        raise MixedScenariosFileNotFound(mixed_scenarios_desc_file)

    mixed_scenarios_desc = yaml.load(mixed_scenarios_desc_file.read_text(), Loader=yaml.FullLoader)

    required_fields = {"overridden_ius", "scenario_name", "disease"}
    missing_fields = required_fields - mixed_scenarios_desc.keys()
    if missing_fields:
        raise MissingFieldsError(missing_fields)

    if not isinstance(mixed_scenarios_desc.get("overridden_ius"), dict):
        raise InvalidOverriddenIUsError()

    if (
        isinstance(mixed_scenarios_desc.get("overridden_ius"), dict)
        and mixed_scenarios_desc.get("overridden_ius") == {}
    ):
        raise InvalidOverriddenIUsError()

    if mixed_scenarios_desc.get("disease") not in ["oncho", "lf", "trachoma"]:
        raise InvalidDiseaseFieldError(mixed_scenarios_desc["disease"], {"oncho", "lf", "trachoma"})

    if "threshold" in mixed_scenarios_desc:
        try:
            threshold = float(mixed_scenarios_desc["threshold"])
        except ValueError as e:
            raise ValueError(f"Threshold must be a number: {e}")

        if threshold < 0.0 or threshold > 1.0:
            raise InvalidThresholdError(threshold)

    try:
        _find_duplicate_ius(mixed_scenarios_desc["overridden_ius"])
    except DuplicateIUError as e:
        raise e

    return MixedScenariosDescription.from_dict(mixed_scenarios_desc)


def _get_pipeline_config_from_scenario_file(
    mixed_scenarios_desc: MixedScenariosDescription,
) -> PipelineConfig:
    # We're guaranteed that the disease value exists and is valid because `mixed_scenarios_desc`
    # has already been verified by this point.
    if mixed_scenarios_desc.disease == "oncho":
        disease = Disease.ONCHO
    elif mixed_scenarios_desc.disease == "trachoma":
        disease = Disease.TRACHOMA
    else:
        disease = Disease.LF

    # We're guaranteed that the threshold is valid, if it exists, because `mixed_scenarios_desc`
    # has already been verified by this point.
    if mixed_scenarios_desc.threshold is not None:
        return PipelineConfig(disease=disease, threshold=mixed_scenarios_desc.threshold)
    else:
        return PipelineConfig(disease=disease)


def _collect_source_target_paths(
    input_canonical_results_dir: Path,
    output_scenario_directory: Path,
    mixed_scenarios_desc: MixedScenariosDescription,
) -> List[Tuple[Path, Path]]:
    """
    Collect all source-to-destination mappings in one pass for the default scenario and overridden
     IUs.

    :param input_canonical_results_dir: Path to the base canonical_results directory.
    :param output_scenario_directory: Path to the output scenario directory.
    :param mixed_scenarios_desc: Dictionary describing the mixed scenarios.
    :return: List of tuples containing (source_path, destination_path).
    """
    paths_to_copy = []

    # Add default scenario directory
    if mixed_scenarios_desc.default_scenario:
        default_scenario_source = (
            input_canonical_results_dir /
            mixed_scenarios_desc.default_scenario
        )
        paths_to_copy.append((default_scenario_source, output_scenario_directory))

    # Add overridden IU directories
    for scenario, ius in mixed_scenarios_desc.overridden_ius.items():
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
                dst.mkdir(parents=True, exist_ok=True)
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


def _prepare_output_directory(
    input_directory: Path,
    output_directory: Path,
    mixed_scenarios_desc: MixedScenariosDescription,
):
    """
    Prepare the output directory structure, copy files, and rename them during copying.

    :param input_directory: Path to the directory containing the input canonical results.
    :param output_directory: Path to the output directory. This directory will be created if
     it does not exist.
    :param mixed_scenarios_desc: Dictionary with mixed scenarios description.
    """
    target_scenario_name = mixed_scenarios_desc.scenario_name

    input_canonical_results_dir = input_directory / "canonical_results"
    output_canonical_results_dir = output_directory / "canonical_results"
    output_canonical_results_dir.mkdir(parents=True, exist_ok=True)

    output_scenario_directory = output_canonical_results_dir / target_scenario_name
    output_scenario_directory.mkdir(parents=True, exist_ok=True)

    # Collect all source and target paths
    paths_to_copy = _collect_source_target_paths(
        input_canonical_results_dir, output_scenario_directory, mixed_scenarios_desc
    )

    def rename_scenario_column(path_to_iu: Path):
        df = pd.read_csv(path_to_iu)
        df["scenario"] = target_scenario_name
        df.to_csv(path_to_iu, index=False)

    # Perform the copy and renaming in a single pass
    _copy_with_rename(
        paths_to_copy,
        r"scenario_\w+_",
        f"{target_scenario_name}_",
        rename_scenario_column,
    )

    # Write mixed_scenarios_desc to a JSON file in the output directory
    mixed_scenarios_metadata_path = output_directory / "mixed_scenarios_metadata.json"
    mixed_scenarios_metadata_path.write_text(mixed_scenarios_desc.to_json())


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
    parser.add_argument(
        "-o",
        "--output-directory",
        type=str,
        required=False,
        help="Path to the output directory. If not specified, the script will create a"
        " new directory in the working directory.",
    )
    parser.add_argument(
        "-s",
        "--scenarios-desc",
        type=lambda p: (
            p
            if p.endswith((".yaml", ".yml"))
            else parser.error("The scenarios description file must be a YAML file (.yaml or .yml).")
        ),
        required=True,
        help="Path to the scenarios description .yaml file.",
    )

    args = parser.parse_args()

    working_directory = Path(args.working_directory).resolve()
    print(f"Working directory: {working_directory}")

    t_start = time.time()
    try:
        mixed_scenarios_desc = _load_mixed_scenarios_desc(Path(args.scenarios_desc))
        print("The mixed scenarios description file has been successfully loaded:")
        pprint(mixed_scenarios_desc, indent=2)
    except (
        MixedScenariosFileNotFound,
        MissingFieldsError,
        InvalidOverriddenIUsError,
        InvalidDiseaseFieldError,
        InvalidThresholdError,
    ) as e:
        print(f"Error: {e}")
        return
    except Exception as e:
        print(f"Unexpected error: {e}")
        return

    try:
        input_directory = _validate_working_directory(working_directory, mixed_scenarios_desc)
    except (
        MissingPopulationMetadataFileError,
        MissingCanonicalResultsDirectoryError,
        MissingScenariosFromSpecificationError,
    ) as e:
        print(f"Error: {e}")
        return
    except Exception as e:
        print(f"Unexpected error: {e}")
        return

    output_directory = (
        Path(args.output_directory) if args.output_directory else working_directory / "output"
    )
    _prepare_output_directory(input_directory, output_directory, mixed_scenarios_desc)
    t_finish = time.time()
    print(f"Time taken to prepare output directory: {t_finish - t_start:.2f} seconds")

    t_start = time.time()

    pipeline_config = _get_pipeline_config_from_scenario_file(mixed_scenarios_desc)

    with CollectAndPrintWarnings() as collected_warnings:
        pipeline.pipeline(
            input_directory,
            output_directory,
            pipeline_config,
        )

        output_directory_structure.write_results_metadata_file(
            output_directory, produce_generation_metadata(warnings=collected_warnings)
        )

    t_finish = time.time()
    print(f"Time taken to run pipeline: {t_finish - t_start:.2f} seconds")


if __name__ == "__main__":
    main()
