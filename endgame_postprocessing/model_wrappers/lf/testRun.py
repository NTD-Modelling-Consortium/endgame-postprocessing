from collections import defaultdict

import pandas as pd
from tqdm import tqdm

from endgame_postprocessing.post_processing import (
    canonicalise,
    output_directory_structure,
    pipeline,
)
from endgame_postprocessing.post_processing import file_util
from endgame_postprocessing.post_processing.canonical_results import CanonicalResults
from endgame_postprocessing.post_processing.disease import Disease
from endgame_postprocessing.post_processing.generation_metadata import produce_generation_metadata
from endgame_postprocessing.post_processing.pipeline_config import PipelineConfig
from endgame_postprocessing.post_processing.replicate_historic_data_from_scenario import \
    replicate_historic_data_in_all_scenarios  # noqa: E501
from endgame_postprocessing.post_processing.warnings_collector import CollectAndPrintWarnings  # noqa: E501


def get_lf_standard(input_dir):
    return file_util.get_flat_regex(
        r"ntdmc-(?P<iu_id>(?P<country>[A-Z]{3})\d{5})-lf-(?P<scenario>scenario_\w+)-200.csv",
        input_dir,
    )


def canonicalise_raw_lf_results(input_dir) -> CanonicalResults:
    file_iter = get_lf_standard(input_dir)

    all_files = list(file_iter)

    if len(all_files) == 0:
        raise Exception(
            "No data for IUs found - see above warnings and check input directory"
        )

    results = defaultdict(dict)

    for file_info in tqdm(all_files, desc="Canoncialise LF results"):
        raw_iu = pd.read_csv(file_info.file_path)
        canonical_result = canonicalise.canonicalise_raw(
            raw_iu, file_info, "sampled mf prevalence (all pop)"
        )
        results[file_info.scenario][file_info.iu] = (file_info, canonical_result)
    return results


def write_canonical_results(results: CanonicalResults, output_dir):
    for scenario in results:
        for iu in results[scenario]:
            file_info, canonical_result = results[scenario][iu]
            output_directory_structure.write_canonical(
                output_dir, file_info, canonical_result
            )


def run_postprocessing_pipeline(
        forward_projection_raw: str,
        scenario_with_historic_data: str,
        output_dir: str,
        num_jobs: int,
):
    """
    Aggregates into standard format the input files found in forward_projection_raw.
    Data specified in scenario_with_historic_data will be prepended to all other scenarios
    (use None to disable this behaviour)
    forward_projection_raw should contain files (in any structure with names matching):
        - `ntdmc-AAA12345-lf-scenario_0-200.csv`
            for IU AAA12345 and scenario 0
        - `PopulationMetadatafile.csv` (must be at the root)

    The output directory must be empty.
    On completion the sub-structure will be:
    output_dir/
        ius/
            a csv per IU with name format
            scenario1_iu1_post_processed.csv
        aggregated/
            combined-lf-iu-lvl-agg.csv - all IUs in one csv
                a aggregated by country csv
            combined-lf-country-lvl-agg.csv - aggregate by country
            combined-lf-africa-lvl-agg.csv - aggregated across Africa
    Arguments:
        forward_projection_raw (str): The directory to search for input files.
        scenario_with_historic_data (str): The name of the scenario to use for historic data
        output_dir (str): The directory to store the output files.

    """
    with CollectAndPrintWarnings() as collected_warnings:
        results = canonicalise_raw_lf_results(forward_projection_raw)
        if scenario_with_historic_data is not None:
            results = replicate_historic_data_in_all_scenarios(results, scenario_with_historic_data)
        write_canonical_results(results, output_dir)

        pipeline.pipeline(
            forward_projection_raw, output_dir, PipelineConfig(disease=Disease.LF)
        )

    output_directory_structure.write_results_metadata_file(
        output_dir,
        produce_generation_metadata(warnings=collected_warnings))


if __name__ == "__main__":
    run_postprocessing_pipeline(
        "local_data/lf",
        "local_data/LF_PrevalenceData_Nov_2024",
        "local_data/lf-output-combined",
        1,
    )
