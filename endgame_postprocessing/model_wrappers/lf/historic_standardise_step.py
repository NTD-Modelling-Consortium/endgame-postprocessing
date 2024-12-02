import glob
from pathlib import Path
import re
import warnings

import pandas as pd
from endgame_postprocessing.model_wrappers.lf.testRun import canonicalise_raw_lf_results
from endgame_postprocessing.post_processing import file_util, pipeline
from endgame_postprocessing.post_processing.dataclasses import CustomFileInfo
from endgame_postprocessing.post_processing.disease import Disease
from endgame_postprocessing.post_processing.pipeline_config import PipelineConfig


def get_lf_matt_flat(input_dir):
    return file_util.get_flat_regex(
        r"(?P<iu_id>(?P<country>[A-Z]{3})\d{5})(?P<scenario>).csv",
        input_dir,
        # Explicitly exclude all files starting with a .
        # as the zip Matt sent over contains loads of spurious files
        glob_expression="**/[!.]*.csv",
    )


def perform_historic_standardise_step(nonstandard_input, raw_output):
    """
    Matt ran the LF model for historic data, but the CSV is very spartan
    the name is just the IU ID, it doesn't specify what the measure is
    or include any of the standard columns.
    This is a custom function to fix this up by:
    Adding measure column with measure set to true mf prevalence (all pop) for all rows
    Adding a age_start and age_end with 5-100
    Renaming the year to the conventional year_id
    Naming the file according to the usual convention, labelling as scenario_0
    """
    Path(raw_input_data).mkdir(parents=True, exist_ok=True)
    for matt_file in get_lf_matt_flat(input_dir):
        raw_iu = pd.read_csv(matt_file.file_path)
        raw_iu.insert(2, "measure", "true mf prevalence (all pop)")
        raw_iu.insert(2, "age_start", 5)
        raw_iu.insert(2, "age_end", 100)
        raw_iu_renamed = raw_iu.rename(columns={"year": "year_id"})
        raw_iu_renamed.to_csv(
            f"{raw_input_data}/ntdmc-{matt_file.iu}-lf-scenario_0-200.csv",
            index=False,
            float_format="%g",
        )


if __name__ == "__main__":
    input_dir = "local_data/LF_PrevalenceData_Nov_2024"
    raw_input_data = "local_data/lf-historic-raw"
    perform_historic_standardise_step(input_dir, raw_input_data)
