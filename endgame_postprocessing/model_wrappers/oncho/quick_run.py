import itertools
from multiprocessing import Pool
import os

import pandas as pd
from endgame_postprocessing.post_processing import (
    canonicalise,
    file_util,
    output_directory_structure,
    pipeline,
)
from endgame_postprocessing.post_processing.custom_file_info import CustomFileInfo
from endgame_postprocessing.post_processing.disease import Disease
from endgame_postprocessing.post_processing.generation_metadata import (
    produce_generation_metadata,
)
from endgame_postprocessing.post_processing.pipeline_config import PipelineConfig
from endgame_postprocessing.post_processing.warnings_collector import (
    CollectAndPrintWarnings,
)


def canonicalise_one(file_info):
    output_dir = "local_data/epioncho-2025-03-04"
    start_year = 2023
    stop_year = 2040
    with CollectAndPrintWarnings() as collected_warnings:
        raw_iu = pd.read_csv(file_info.file_path)
        raw_iu_filtered = raw_iu[
            (raw_iu["year_id"] >= start_year) & (raw_iu["year_id"] <= stop_year)
        ]
        canonical_result = canonicalise.canonicalise_raw(
            raw_iu_filtered, file_info, "prevalence"
        )
        output_directory_structure.write_canonical(
            output_dir, file_info, canonical_result
        )
    return collected_warnings


if __name__ == "__main__":
    input_dir = "/home/thomas/dev/data/epioncho-old-data/ntdmc-data-wrangler/output"
    all_files = sorted(
        list(
            file_util.get_flat_regex(
                r"ntdmc-(?P<iu_id>(?P<country>[A-Z]{3})\d{5})-(?P<scenario>old_scenario_-?\w+)-200_sims.csv",
                input_dir,
            )
        )
    )
    # all_files = sorted(
    #     list(
    #         file_util.get_flat_regex(
    #             r"ntdmc-(?P<iu_id>(?P<country>TGO)\d{5})-(?P<scenario>old_scenario_\w+)-200_sims.csv",
    #             input_dir,
    #         )
    #     )
    # )

    if len(all_files) == 0:
        raise Exception(
            "No data for IUs found - see above warnings and check input directory"
        )
    os.makedirs("local_data/epioncho-2025-03-04", exist_ok=True)
    with Pool() as p:
        warnings_from_each_iu = p.map(canonicalise_one, all_files)
    all_canoncical_warnings = list(itertools.chain.from_iterable(warnings_from_each_iu))
    # all_canoncical_warnings = list(itertools.chain.from_iterable([]))
    output_dir = "local_data/epioncho-2025-03-04"
    with CollectAndPrintWarnings() as collected_warnings:
        pipeline.pipeline(input_dir, output_dir, PipelineConfig(disease=Disease.ONCHO))
    all_warnings = all_canoncical_warnings + collected_warnings
    output_directory_structure.write_results_metadata_file(
        output_dir, produce_generation_metadata(warnings=all_warnings)
    )
