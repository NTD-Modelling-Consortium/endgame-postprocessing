from pathlib import Path

import pandas as pd
from endgame_postprocessing.post_processing import file_util


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
    Adding measure column with measure set to sampled mf prevalence (all pop) for all rows
    Adding a age_start and age_end with 5-100
    Renaming the year to the conventional year_id
    Naming the file according to the usual convention, labelling as scenario_0
    """
    Path(raw_output).mkdir(parents=True, exist_ok=True)
    for matt_file in get_lf_matt_flat(nonstandard_input):
        raw_iu = pd.read_csv(matt_file.file_path)
        raw_iu.insert(2, "measure", "sampled mf prevalence (all pop)")
        raw_iu.insert(2, "age_start", 5)
        raw_iu.insert(2, "age_end", 100)
        raw_iu_renamed = raw_iu.rename(columns={"year": "year_id"})
        raw_iu_renamed.to_csv(
            f"{raw_output}/ntdmc-{matt_file.iu}-lf-scenario_0-200.csv",
            index=False,
            float_format="%g",
        )
