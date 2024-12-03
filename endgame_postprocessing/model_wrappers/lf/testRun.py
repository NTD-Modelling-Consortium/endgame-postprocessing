from tqdm import tqdm
from endgame_postprocessing.model_wrappers.lf import combine_historic_and_forward
from endgame_postprocessing.model_wrappers.lf.historic_standardise_step import (
    perform_historic_standardise_step,
)
from endgame_postprocessing.post_processing import (
    canonicalise,
    output_directory_structure,
    pipeline,
)
from endgame_postprocessing.post_processing import file_util
from endgame_postprocessing.post_processing.disease import Disease
import pandas as pd

from endgame_postprocessing.post_processing.pipeline_config import PipelineConfig


def get_lf_standard(input_dir):
    return file_util.get_flat_regex(
        r"ntdmc-(?P<iu_id>(?P<country>[A-Z]{3})\d{5})-lf-(?P<scenario>scenario_\d+)-200.csv",
        input_dir,
    )


def canonicalise_raw_lf_results(input_dir, output_dir):
    file_iter = get_lf_standard(input_dir)

    all_files = list(file_iter)

    if len(all_files) == 0:
        raise Exception(
            "No data for IUs found - see above warnings and check input directory"
        )

    for file_info in tqdm(all_files, desc="Canoncialise LF results"):
        raw_iu = pd.read_csv(file_info.file_path)
        canonical_result = canonicalise.canonicalise_raw(
            raw_iu, file_info, "true mf prevalence (all pop)"
        )
        output_directory_structure.write_canonical(
            output_dir, file_info, canonical_result
        )


def run_postprocessing_pipeline(
    forward_projection_raw: str,
    historic_data_nonstandard: str,
    output_dir: str,
    num_jobs: int,
):
    """
    Aggregates into standard format the input files found in input_dir.
    input_dir must have the following substructure:
        scenario1/
            country1/
                iu1/
                    iu.csv
        scenario2/

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
        input_dir (str): The directory to search for input files.
        output_dir (str): The directory to store the output files.

    """
    if historic_data_nonstandard is not None:
        historic_raw_path = f"{historic_data_nonstandard}/raw"
        perform_historic_standardise_step(historic_data_nonstandard, historic_raw_path)

        forward_canonical = f"{forward_projection_raw}/canonical"
        canonicalise_raw_lf_results(forward_projection_raw, forward_canonical)
        historic_canonical = f"{historic_raw_path}/canonical"
        canonicalise_raw_lf_results(historic_raw_path, historic_canonical)

        combine_historic_and_forward.combine_historic_and_forward(
            historic_canonical, forward_canonical, output_dir
        )
    else:
        canonicalise_raw_lf_results(forward_projection_raw, output_dir)

    pipeline.pipeline(
        forward_projection_raw, output_dir, PipelineConfig(disease=Disease.LF)
    )


if __name__ == "__main__":
    run_postprocessing_pipeline(
        "local_data/lf",
        "local_data/LF_PrevalenceData_Nov_2024",
        "local_data/lf-output-combined",
        1,
    )
