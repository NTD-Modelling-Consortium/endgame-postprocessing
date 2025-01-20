from os import PathLike
from pathlib import Path
from typing import Optional

import pandas as pd
from tqdm import tqdm

from endgame_postprocessing.post_processing import (
    canonicalise,
    output_directory_structure,
    pipeline,
    file_util,
    canoncical_columns,
)
from endgame_postprocessing.post_processing.disease import Disease
from endgame_postprocessing.post_processing.file_util import get_matching_csv
from endgame_postprocessing.post_processing.generation_metadata import (
    produce_generation_metadata,
)
from endgame_postprocessing.post_processing.pipeline_config import PipelineConfig
from endgame_postprocessing.post_processing.warnings_collector import (
    CollectAndPrintWarnings,
)


def canonicalise_raw_trachoma_results(
    input_dir: str | PathLike | Path,
    output_dir: str | PathLike | Path,
    historic_dir: Optional[str | PathLike | Path] = None,
    historic_prefix: str = "",
    start_year: int = 1970,
    stop_year: int = 2041,
):
    file_iter = file_util.get_flat_regex(
        file_name_regex=r"ntdmc-(?P<iu_id>(?P<country>[A-Z]{3})\d{5})-(?P<disease>\w+)-(?P<scenario>scenario_\w+)-200(\S*\w*).csv",
        input_dir=input_dir,
    )

    all_files = list(file_iter)

    if len(all_files) == 0:
        # TODO: Define and use exception type. For eg. IUMissingException
        raise Exception(
            "No data for IUs found - see above warnings and check input directory"
        )

    for file_info in tqdm(all_files, desc="Canonicalise Trachoma results"):
        raw_iu = pd.read_csv(file_info.file_path)

        # TODO(16.1.2025): Implement historic data handling here
        if historic_dir is not None:
            historic_iu_file_path = get_matching_csv(
                historic_dir,
                historic_prefix,
                file_info.country,
                file_info.iu.replace(file_info.country, ""),
            )
            raw_iu_historic = pd.read_csv(historic_iu_file_path)
            raw_iu = pd.concat([raw_iu_historic, raw_iu])

        raw_iu_filtered = raw_iu[
            (raw_iu["Time"] >= start_year) & (raw_iu["Time"] <= stop_year)
        ].copy()

        raw_iu_filtered = raw_iu_filtered.rename(
            columns={"Time": canoncical_columns.YEAR_ID}
        )

        # TODO: canonical shouldn't need the age_start / age_end but these are assumed present later
        canonical_result = canonicalise.canonicalise_raw(
            raw=raw_iu_filtered,
            file_info=file_info,
            processed_prevalence_name="prevalence",
        )
        output_directory_structure.write_canonical(
            output_dir, file_info, canonical_result
        )


def run_postprocessing_pipeline(
    input_dir: str | PathLike | Path,
    output_dir: str | PathLike | Path,
    historic_dir: Optional[str | PathLike | Path] = None,
    historic_prefix: str = "",
    start_year: int = 1970,
    stop_year: int = 2041,
):
    with CollectAndPrintWarnings() as collected_warnings:
        canonicalise_raw_trachoma_results(
            input_dir=input_dir,
            output_dir=output_dir,
            historic_dir=historic_dir,
            historic_prefix=historic_prefix,
            start_year=start_year,
            stop_year=stop_year,
        )

        pipeline.pipeline(
            input_dir=input_dir,
            working_directory=output_dir,
            pipeline_config=PipelineConfig(disease=Disease.TRACHOMA, threshold=0.05),
        )

    output_directory_structure.write_results_metadata_file(
        output_dir, produce_generation_metadata(warnings=collected_warnings)
    )
