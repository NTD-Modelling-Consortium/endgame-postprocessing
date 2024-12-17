from tqdm import tqdm
from endgame_postprocessing.post_processing import (
    canonicalise,
    output_directory_structure,
    pipeline,
)
from endgame_postprocessing.post_processing.disease import Disease
from endgame_postprocessing.post_processing.file_util import (
    post_process_file_generator,
)
import pandas as pd

from endgame_postprocessing.post_processing.pipeline_config import PipelineConfig


def canonicalise_raw_oncho_results(input_dir, output_dir, start_year=1970, stop_year=2041):
    file_iter = post_process_file_generator(
        file_directory=input_dir, end_of_file=".csv"
    )

    all_files = list(file_iter)

    if len(all_files) == 0:
        raise Exception(
            "No data for IUs found - see above warnings and check input directory"
        )

    for file_info in tqdm(all_files, desc="Canoncialise Oncho results"):
        raw_iu = pd.read_csv(file_info.file_path)
        raw_iu_filtered = raw_iu[
            (raw_iu['year_id'] >= start_year) & (raw_iu['year_id'] <= stop_year)
        ].copy()
        # TODO: canonical shouldn't need the age_start / age_end but these are assumed present later
        canonical_result = canonicalise.canonicalise_raw(
            raw_iu_filtered, file_info, "prevalence"
        )
        output_directory_structure.write_canonical(
            output_dir, file_info, canonical_result
        )


def run_postprocessing_pipeline(input_dir: str, output_dir: str):
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
            combined-oncho-iu-lvl-agg.csv - all IUs in one csv
                a aggregated by country csv
            combined-oncho-country-lvl-agg.csv - aggregate by country
            combined-oncho-africa-lvl-agg.csv - aggregated across Africa
    Arguments:
        input_dir (str): The directory to search for input files.
        output_dir (str): The directory to store the output files.

    """
    canonicalise_raw_oncho_results(input_dir, output_dir)
    pipeline.pipeline(input_dir, output_dir, PipelineConfig(disease=Disease.ONCHO))

if __name__ == "__main__":
    run_postprocessing_pipeline("local_data/oncho", "local_data/oncho-output")
