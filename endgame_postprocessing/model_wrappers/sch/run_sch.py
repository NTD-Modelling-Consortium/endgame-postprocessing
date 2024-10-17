from functools import reduce
from operator import mul
import os
import re
import warnings
from tqdm import tqdm
from endgame_postprocessing.post_processing import (
    canonicalise,
    output_directory_structure,
    pipeline,
)
from endgame_postprocessing.post_processing.dataclasses import CustomFileInfo
from endgame_postprocessing.post_processing.disease import Disease
from endgame_postprocessing.post_processing.file_util import (
    post_process_file_generator,
)
import pandas as pd

def probability_any_worm(probability_for_each_worm):
    """
    Calculate the probability of having any worm
    """
    prob_of_not_each_worm = map(
        lambda prob_having_worm: 1.0 - prob_having_worm, probability_for_each_worm
    )
    prob_not_any_worm = reduce(mul, prob_of_not_each_worm, 1.0)
    return 1.0 - prob_not_any_worm


def canoncialise_single_result(file_info):
    raw_iu = pd.read_csv(file_info.file_path)
    raw_without_columns = raw_iu.drop(columns=["intensity", "species"])
    # TODO: canonical shouldn't need the age_start / age_end but these are assumed present later
    return canonicalise.canonicalise_raw(
        raw_without_columns, file_info, "Prevalence SAC"
    )


def combine_many_worms(first_worm, other_worms):
    other_worm_draws = [other_worm.loc[:, "draw_0":] for other_worm in other_worms]
    first_worm.loc[:, "draw_0":] = probability_any_worm(
        [first_worm.loc[:, "draw_0":]] + other_worm_draws
    )
    return first_worm


# def get_flat(input_dir):
#     path, directories, files = next(os.walk(input_dir))
#     for file in files:
#         file_name_regex = r"ntdmc-(?P<country>[A-Z]{3})(?P<iu_id>\d{5})-(\w+)-group_001-(?P<scenario>scenario_(?P<scenario_index>\d))-group_001-200_simulations.csv"
#         file_match = re.match(file_name_regex, file)
#         if not file_match:
#             warnings.warns(f"Unexpected file: {file}")
#             continue

#         yield CustomFileInfo(
#             scenario_index=int(file_match("scenario_index")),
#             total_scenarios=3,  # TODO
#             scenario=file_match.group("scenario"),
#             country=file_match.group("country"),

#         )


def canonicalise_raw_sth_results(input_dir, output_dir):
    worms = next(os.walk(input_dir))[1]

    first_worm = worms[0]
    other_worms = worms[1:]
    file_iter = post_process_file_generator(
        file_directory=f"{input_dir}/{first_worm}", end_of_file=".csv"
    )

    all_files = list(file_iter)

    if len(all_files) == 0:
        raise Exception(
            "No data for IUs found - see above warnings and check input directory"
        )

    for file_info in tqdm(all_files, desc="Canoncialise STH results"):
        canonical_result_first_worm = canoncialise_single_result(file_info)
        other_worm_paths = [
            file_info.file_path.replace(first_worm, worm) for worm in other_worms
        ]

        other_worm_file_infos = [
            CustomFileInfo(
                scenario_index=file_info.scenario_index,
                scenario=file_info.scenario,
                country=file_info.country,
                file_path=other_worm_path,
                iu=file_info.iu,
                total_scenarios=file_info.total_scenarios,
            )
            for other_worm_path in other_worm_paths
        ]

        other_worms_canoncial = [
            canoncialise_single_result(other_worm_file_info)
            for other_worm_file_info in other_worm_file_infos
        ]

        all_worms_canonical = combine_many_worms(
            canonical_result_first_worm, other_worms_canoncial
        )
        output_directory_structure.write_canonical(
            output_dir, file_info, all_worms_canonical
        )


def run_postprocessing_pipeline(input_dir: str, output_dir: str, num_jobs: int):
    """
    Aggregates into standard format the input files found in input_dir.
    input_dir must have the following substructure:
        PopulationMetadatafile.csv
        worm1/
            scenario1/
                country1/
                    iu1/
                        iu.csv
            scenario2/
        worm2/

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

    Note this will be looking at prevalence across any worm.

    """
    canonicalise_raw_sth_results(input_dir, output_dir)
    pipeline.pipeline(input_dir, output_dir, disease=Disease.STH)


if __name__ == "__main__":
    run_postprocessing_pipeline("local_data/sth-small", "local_data/sth-output", 1)
