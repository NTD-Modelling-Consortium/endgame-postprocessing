import warnings
from tqdm import tqdm
from endgame_postprocessing.post_processing import (
    canonicalise,
    output_directory_structure,
    pipeline,
)
from endgame_postprocessing.post_processing.disease import Disease
from endgame_postprocessing.post_processing.file_util import (
    post_process_file_generator,
    get_matching_csv,
    list_all_historic_ius,
)
import pandas as pd

from endgame_postprocessing.post_processing.pipeline_config import PipelineConfig


def canonicalise_raw_oncho_results(
        input_dir, output_dir,
        start_year=1970, stop_year=2041,
        historic_dir=None, historic_prefix = ""
):
    file_iter = post_process_file_generator(
        file_directory=input_dir, end_of_file=".csv"
    )

    all_files = list(file_iter)

    if len(all_files) == 0:
        raise Exception(
            "No data for IUs found - see above warnings and check input directory"
        )

    all_historic_ius = list_all_historic_ius(
        historic_dir,
        historic_prefix
    )
    excluded_ius = {
        "not_in_historic": set(),
        "not_in_forward_projections": set()
    }
    for file_info in tqdm(all_files, desc="Canoncialise Oncho results"):
        raw_iu = pd.read_csv(file_info.file_path)
        if historic_dir is not None:
            # Note: for oncho the historic files have no folder structure
            # The IU names in the historic files use the long code.
            # The IU parameter in the file_info object contains the country code, which we need
            # to remove to properly search
            # See: https://github.com/NTD-Modelling-Consortium/endgame-project/issues/166
            historic_iu_file_path = get_matching_csv(
                historic_dir,
                historic_prefix,
                file_info.country,
                file_info.iu.replace(file_info.country, ""),
                file_info.scenario
            )
            if (historic_iu_file_path is None):
                excluded_ius["not_in_historic"].add(file_info.iu)
                continue
            else:
                if (file_info.iu in all_historic_ius):
                    all_historic_ius.remove(file_info.iu)
            raw_iu_historic = pd.read_csv(historic_iu_file_path)
            raw_iu = pd.concat([raw_iu_historic, raw_iu])
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
    for iu in all_historic_ius:
        excluded_ius["not_in_forward_projections"].add(iu)
        warnings.warn(
            f"IU {iu} was not found in forward_projections " +
            "and as such will not have the historic data"
        )


def run_postprocessing_pipeline(
        input_dir: str, output_dir: str,
        historic_dir: str = None, historic_prefix: str ="*",
        start_year=1970, stop_year=2041,
    ):
    """
    Aggregates into standard format the input files found in input_dir.
    input_dir must have the following substructure:
        scenario1/
            country1/
                iu1/
                    iu.csv
        scenario2/

    historic_dir does not need a specific structure, however if provided,
    all IUs in input_dir, must have ONLY 1 related file in the historic_dir

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
        historic_dir (str, optional): The directory to search for historic IU data.
            Defaults to None.
        historic_prefix (str, optional): The prefix for the historic IU files.
            This is the value that comes before the country code, i.e raw_outputs_ if the file name
            is raw_outputs_AAAXXXX00002. Defaults to "*".
        start_year: The first year to be included in the results
        stop_year: The last year to be included in the results

    """
    canonicalise_raw_oncho_results(
        input_dir, output_dir,
        historic_dir=historic_dir, historic_prefix=historic_prefix,
        start_year=start_year, stop_year=stop_year
    )
    pipeline.pipeline(input_dir, output_dir, PipelineConfig(disease=Disease.ONCHO))

if __name__ == "__main__":
    run_postprocessing_pipeline("local_data/oncho",
                                "local_data/oncho-output",
                                historic_dir="local_data/historic-oncho",
                                historic_prefix="raw_outputs_"
    )
