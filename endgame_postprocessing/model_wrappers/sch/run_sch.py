from functools import reduce
import glob
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
import pandas as pd

from endgame_postprocessing.post_processing.pipeline_config import PipelineConfig

WORM_MAPPING = {
    "hookworm": "hookworm",
    "ascaris": "roundworm",
    "trichuris": "whipworm",
}

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


def swap_worm_in_heirachy(original_file_info, first_worm, new_worm):
    friendly_first_worm = WORM_MAPPING[first_worm]
    friendly_new_worm = WORM_MAPPING[new_worm]
    other_worm_path = original_file_info.file_path.replace(
        first_worm, new_worm
    ).replace(friendly_first_worm, friendly_new_worm)

    return CustomFileInfo(
        scenario_index=original_file_info.scenario_index,
        scenario=original_file_info.scenario,
        country=original_file_info.country,
        file_path=other_worm_path,
        iu=original_file_info.iu,
        total_scenarios=original_file_info.total_scenarios,
    )


def _get_flat_regex(file_name_regex, input_dir):
    files = glob.glob(
        "**/ntdmc-*-group_001-200_simulations.csv", root_dir=input_dir, recursive=True
    )
    for file in files:
        file_match = re.search(file_name_regex, file)
        if not file_match:
            warnings.warn(f"Unexpected file: {file}")
            continue

        yield CustomFileInfo(
            scenario_index=1,  # TODO - note scenarios are not ints, eg 2a
            total_scenarios=3,  # TODO
            scenario=file_match.group("scenario"),
            country=file_match.group("country"),
            iu=file_match.group("iu_id"),
            file_path=f"{input_dir}/{file}",
        )


def get_sth_flat(input_dir):
    return _get_flat_regex(
        r"ntdmc-(?P<iu_id>(?P<country>[A-Z]{3})\d{5})-(?P<worm>\w+)-group_001-(?P<scenario>scenario_\w+)-group_001-200_simulations.csv",
        input_dir,
    )


def get_sth_worm(file_path):
    file_name_regex = r"ntdmc-[A-Z]{3}\d{5}-(?P<worm>\w+)-group_001-scenario_\w+-group_001-200_simulations.csv"  # noqa 501
    file_match = re.search(file_name_regex, file_path)
    return file_match.group("worm")


def canonicalise_raw_sth_results(input_dir, output_dir, worm_directories):
    if len(worm_directories) == 0:
        raise Exception("Must provide at least one worm directory")
    first_worm_dir = worm_directories[0]

    if not os.path.exists(f"{input_dir}/{first_worm_dir}"):
        raise Exception(
            f"Could not find worm directory {first_worm_dir} inside {input_dir}"
        )

    other_worms_dirs = worm_directories[1:]
    other_worms = [
        get_sth_worm(next(get_sth_flat(f"{input_dir}/{other_worm_dir}")).file_path)
        for other_worm_dir in other_worms_dirs
    ]
    # file_iter = post_process_file_generator(
    #     file_directory=f"{input_dir}/{first_worm_dir}", end_of_file=".csv"
    # )
    file_iter = get_sth_flat(f"{input_dir}/{first_worm_dir}")

    all_files = list(file_iter)

    if len(all_files) == 0:
        raise Exception(
            "No data for IUs found - see above warnings and check input directory"
        )

    for file_info in tqdm(all_files, desc="Canoncialise STH results"):
        canonical_result_first_worm = canoncialise_single_result(file_info)
        first_worm = get_sth_worm(file_info.file_path)
        other_worm_file_infos = [
            swap_worm_in_heirachy(file_info, first_worm, worm) for worm in other_worms
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


def run_postprocessing_pipeline(
    input_dir: str,
    output_dir: str,
    worm_directories: list[str],
    num_jobs: int,
    skip_canonical=False,
):
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
        worm_directories (list[str]) The worm directories within input_dir
           to combine. Provide a single worm directory to process a single worm

    Note this will be looking at prevalence across any worm.

    """
    if not skip_canonical:
        canonicalise_raw_sth_results(input_dir, output_dir, worm_directories)

    config = PipelineConfig(
        disease=Disease.STH,
        threshold=0.1,
        include_country_and_continent_summaries=False,
    )
    pipeline.pipeline(input_dir, output_dir, config)


if __name__ == "__main__":
    input_dir = "local_data/sth-fresh"
    worm_directories = next(os.walk(input_dir))[1]
    for worm_directory in worm_directories:
        run_postprocessing_pipeline(
            input_dir,
            f"local_data/sth-output-single-worm-small/{worm_directory}",
            [worm_directory],
            1,
        )
