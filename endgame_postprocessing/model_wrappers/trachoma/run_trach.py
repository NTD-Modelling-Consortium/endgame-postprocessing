import warnings
from collections import namedtuple
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
from endgame_postprocessing.post_processing.generation_metadata import (
    produce_generation_metadata,
)
from endgame_postprocessing.post_processing.pipeline_config import PipelineConfig
from endgame_postprocessing.post_processing.warnings_collector import (
    CollectAndPrintWarnings,
)

DiscoveredIUs = namedtuple(
    "DiscoveredIUs",
    ["all_forward", "all_historic", "forward_only", "history_only", "with_history"],
    defaults=(set() for _ in range(5)),
)


def _discover_ius(
        forward_projections_dir: str | PathLike | Path,
        forward_projections_file_name_regex: str,
        historic_dir: Optional[str | PathLike | Path] = None,
        historic_prefix: str = "",
) -> DiscoveredIUs:
    """
    Finds five sets of IUs -
        1. All forward projection IUs.
        2. All historic IUs.
        3. IUs that exist only in forward projection data but no history.
        4. IUs that exist only in historic data but not associated forward projections.
        5. IUs that exist in both forward projection and historic data.
    """

    all_forward_ius = {
        fp.iu: fp
        for fp in file_util.get_flat_regex(
            file_name_regex=forward_projections_file_name_regex,
            input_dir=forward_projections_dir,
        )
    }

    # TODO: This can also be a iterator over CustomFileInfo with only a subset of the fields filled out
    all_historic_ius = file_util.list_all_historic_ius(historic_dir, historic_prefix)

    set_all_forward = set(all_forward_ius.keys())
    set_all_historic = set(all_historic_ius.keys())

    return DiscoveredIUs(
        all_forward=all_forward_ius,
        all_historic=all_historic_ius,
        forward_only={
            iu: all_forward_ius[iu] for iu in set_all_forward - set_all_historic
        },
        history_only={
            iu: all_historic_ius[iu] for iu in set_all_historic - set_all_forward
        },
        with_history={
            iu: (all_forward_ius[iu], all_historic_ius[iu])
            for iu in set_all_forward & set_all_historic
        },
    )


def canonicalise_raw_trachoma_results(
        input_dir: str | PathLike | Path,
        output_dir: str | PathLike | Path,
        historic_dir: Optional[str | PathLike | Path] = None,
        historic_prefix: str = "",
        start_year: int = 1970,
        stop_year: int = 2041,
):
    discovered_ius = _discover_ius(
        forward_projections_dir=input_dir,
        forward_projections_file_name_regex=r"ntdmc-(?P<iu_id>(?P<country>[A-Z]{3})\d{5})-(?P<disease>\w+)-(?P<scenario>scenario_\w+)-200(.*).csv",
        historic_dir=historic_dir,
        historic_prefix=historic_prefix,
    )

    if not discovered_ius.all_forward:
        # TODO: Define and use exception type. For eg. IUMissingException
        raise Exception(
            "No data for IUs found - see above warnings and check input directory"
        )

    if not discovered_ius.all_historic:
        """NOTE
        There aren't any historic data to prepend to the future projections.
        So we should say that we skip the concatenation altogether, but raise a warning.
        """
        # TODO: Define and use exception type. For eg. HistoricDataMissingException
        warnings.warn(f"No historic IUs found for {historic_prefix} in {historic_dir}")

    """TODO
    Define and use standardized warning messages.
    For the IUs that only exist in either forward only or historic only data, we'll raise a warning
    saying that they'll be excluded from processing
    """

    # Log warnings for IUs found in forward projections but not in histories
    for iu in discovered_ius.forward_only:
        warnings.warn(f"IU {iu} found in forward projections but not found in history.")

    # Log warnings for IUs found in histories but not in forward projections
    for iu in discovered_ius.history_only:
        warnings.warn(f"IU {iu} found in history but not in forward projections.")

    for fp_file_info in tqdm(
            discovered_ius.all_forward.values(), desc="Canonicalise Trachoma results"
    ):
        raw_iu_fp = pd.read_csv(fp_file_info.file_path)

        # Find the matching historic iu file, if it exists, and concatenate it to the forward projection
        if fp_file_info.iu in discovered_ius.all_historic:
            raw_iu_historic = pd.read_csv(discovered_ius.all_historic[fp_file_info.iu])
            raw_iu_fp = pd.concat([raw_iu_historic, raw_iu_fp])

        raw_iu_fp = raw_iu_fp.rename(columns={"Time": canoncical_columns.YEAR_ID})

        raw_iu_filtered = raw_iu_fp[
            (raw_iu_fp[canoncical_columns.YEAR_ID] >= start_year)
            & (raw_iu_fp[canoncical_columns.YEAR_ID] <= stop_year)
            ].copy()

        # TODO: canonical shouldn't need the age_start / age_end but these are assumed present later
        canonical_result = canonicalise.canonicalise_raw(
            raw=raw_iu_filtered,
            file_info=fp_file_info,
            processed_prevalence_name="prevalence",
        )

        output_directory_structure.write_canonical(
            output_dir, fp_file_info, canonical_result
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
