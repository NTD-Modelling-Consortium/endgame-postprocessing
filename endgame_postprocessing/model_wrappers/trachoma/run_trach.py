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
    file_util, canonical_columns,
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

    all_historic_ius = {
        fp.iu: fp
        for fp in file_util.get_flat_regex(
            file_name_regex=r"(?P<prefix>\w+)(?P<iu_id>(?P<country>[A-Z]{3}).{0,5}\d{5})(.*)\.csv",
            input_dir=historic_dir,
            glob_expression=f"{historic_prefix}_*.csv",
        )
    }

    all_forward_set = set(all_forward_ius.keys())
    all_historic_set = set(all_historic_ius.keys())

    return DiscoveredIUs(
        all_forward=all_forward_ius,
        all_historic=all_historic_ius,
        forward_only={
            iu: all_forward_ius[iu] for iu in all_forward_set - all_historic_set
        },
        history_only={
            iu: all_historic_ius[iu] for iu in all_historic_set - all_forward_set
        },
        with_history={
            iu: (all_forward_ius[iu], all_historic_ius[iu])
            for iu in all_forward_set & all_historic_set
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
        warnings.warn(f"No historic IUs found for "
                      f"prefix='{historic_prefix}' in directory='{historic_dir}'")
    else:
        """TODO
        Define and use standardized warning messages.
        For the IUs that only exist in either forward only or historic only data,
        we'll raise a warning saying that they'll be excluded from processing
        """
        # Log warnings for IUs found in forward projections but not in histories
        for iu in discovered_ius.forward_only:
            warnings.warn(f"IU '{iu}' found in forward projections but not found in history.")

        # Log warnings for IUs found in histories but not in forward projections
        for iu in discovered_ius.history_only:
            warnings.warn(f"IU '{iu}' found in history but not in forward projections.")

    def prepend_historic_if_available(fp: CustomFileInfo,
                                      hs: Optional[CustomFileInfo]) -> pd.DataFrame:
        return pd.concat([pd.read_csv(hs.file_path) if hs else pd.DataFrame(),
                          pd.read_csv(fp.file_path)])

    if not discovered_ius.all_historic:
        ius_to_process = [(fp, None) for fp in discovered_ius.all_forward.values()]
    else:
        ius_to_process = list(discovered_ius.with_history.values())

    for fp_fileinfo, hs_fileinfo in tqdm(
            ius_to_process, desc="Canonicalise Trachoma results"
    ):
        output_directory_structure.write_canonical(
            output_dir,
            fp_fileinfo,
            (prepend_historic_if_available(fp=fp_fileinfo, hs=hs_fileinfo)
             .rename(columns={"Time": canonical_columns.YEAR_ID})
             .query(
                f"{canonical_columns.YEAR_ID} >= {start_year}"
                f" and {canonical_columns.YEAR_ID} <= {stop_year}")
             .copy()
             .pipe(canonicalise.canonicalise_raw,
                   file_info=fp_fileinfo,
                   processed_prevalence_name="prevalence")
             )
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
