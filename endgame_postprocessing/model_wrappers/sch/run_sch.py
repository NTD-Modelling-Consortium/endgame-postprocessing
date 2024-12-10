import glob
import os
import re
import warnings
from tqdm import tqdm
from endgame_postprocessing.model_wrappers.sch import probability_any_worm
from endgame_postprocessing.post_processing import (
    canonicalise,
    output_directory_structure,
    pipeline,
)
from endgame_postprocessing.post_processing.dataclasses import CustomFileInfo
from endgame_postprocessing.post_processing.disease import Disease
import pandas as pd
import numpy as np

from endgame_postprocessing.post_processing.pipeline_config import PipelineConfig

WORM_MAPPING = {
    "hookworm": "hookworm",
    "ascaris": "roundworm",
    "trichuris": "whipworm",
    "sch-haematobium": "haematobium",
    "sch-mansoni-high-burden": "mansoni_high_burden",
    "sch-mansoni-low-burden": "mansoni_low_burden",
}

def canoncialise_single_result(file_info, warning_if_no_file=False):
    try:
        raw_iu = pd.read_csv(file_info.file_path)
        raw_without_columns = raw_iu.drop(columns=["intensity", "species"])
        # TODO: canonical shouldn't need the age_start / age_end but these are assumed present later
        return canonicalise.canonicalise_raw(
            raw_without_columns, file_info, "Prevalence SAC"
        )
    except FileNotFoundError:
        if warning_if_no_file:
            warnings.warn(
                f"File {file_info.file_path} not found, and `warning_if_no_file` is set to True"
            )
            return pd.DataFrame()
        raise FileNotFoundError


def combine_many_worms(
        first_worm,
        other_worms,
        combination_function = probability_any_worm.independent_probability):
    if not callable(combination_function):
        raise Exception("Need to provide a callable function to combine worms.")
    other_worm_draws = [
        other_worm.loc[:, "draw_0":]
        if not other_worm.empty
        else pd.DataFrame(
            np.zeros(first_worm.loc[:, "draw_0":].shape),
            columns=first_worm.columns[first_worm.columns.get_loc("draw_0"):]
        )
        for other_worm in other_worms
    ]

    first_worm.loc[:, "draw_0":] = combination_function(
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


def get_sch_flat(input_dir):
    return _get_flat_regex(
        r"ntdmc-(?P<iu_id>(?P<country>[A-Z]{3})\d{5})-(?P<worm>[\w_]+)-group_001-(?P<scenario>scenario_\w+)-survey_type_kk2-group_001-200_simulations.csv",
        input_dir,
    )


def get_sth_worm(file_path):
    file_name_regex = r"ntdmc-[A-Z]{3}\d{5}-(?P<worm>\w+)-group_001-scenario_\w+-group_001-200_simulations.csv"  # noqa 501
    file_match = re.search(file_name_regex, file_path)
    return file_match.group("worm")

def get_sch_worm_info(file_path):
    file_name_regex = r"ntdmc-(?P<iu_id>[A-Z]{3}\d{5})-(?P<worm>[\w]+?)(_(?P<burden>high_burden|low_burden))?-group_001-(?P<scenario>scenario_\w+)-survey_type_kk2-group_001-200_simulations.csv" # noqa 501
    file_match = re.search(file_name_regex, file_path)
    return (
        file_match.group("worm"), file_match.group("burden"),
        file_match.group("iu_id"), file_match.group("scenario")
    )


def canonicalise_raw_sth_results(
        input_dir,
        output_dir,
        worm_directories,
        warning_if_no_file,
        worm_combination_algorithm):
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
            canoncialise_single_result(other_worm_file_info, warning_if_no_file)
            for other_worm_file_info in other_worm_file_infos
        ]

        all_worms_canonical = combine_many_worms(
            canonical_result_first_worm,
            other_worms_canoncial,
            combination_function=worm_combination_algorithm
        )
        output_directory_structure.write_canonical(
            output_dir, file_info, all_worms_canonical
        )

def _check_iu_in_all_folders(worm_iu_info, warning_if_no_file):
    info = {}
    unique_worms = set()
    for worm, _, iu, scenario in worm_iu_info:
        unique_worms.add(worm)
        if scenario not in info:
            info[scenario] = {}
        if iu not in info[scenario]:
            info[scenario][iu] = {}
        if worm not in info[scenario][iu]:
            info[scenario][iu][worm] = 0
        info[scenario][iu][worm] += 1
        if info[scenario][iu][worm] > 1:
            raise Exception(
                f"IU {iu} found multiple times for {worm} in scenario {scenario}."
            )

    for scenario in info.keys():
        for iu in info[scenario].keys():
            iu_worms = info[scenario][iu].keys()
            for worm in unique_worms:
                if worm != "haematobium" and worm not in iu_worms:
                    if warning_if_no_file:
                        warnings.warn(f"IU not present for {worm}")
                    else:
                        raise Exception(
                            f"IU {iu} not present for {worm}."
                        )

def canonicalise_raw_sch_results(
    input_dir,
    output_dir,
    worm_directories,
    warning_if_no_file
):
    if len(worm_directories) < 1:
        raise Exception(
            "Expected at least 1 item in the worm_directories parameter," +
            f"received {len(worm_directories)}."
        )
    # Assuming that the first worm only has one burden
    first_worm = worm_directories[0]
    file_iter = get_sch_flat(f"{input_dir}{first_worm}")

    all_iu_worm_info = [
        get_sch_worm_info(file_info.file_path)
        for worm_dir in worm_directories
        for file_info in get_sch_flat(f"{input_dir}{worm_dir}")
    ]
    _check_iu_in_all_folders(all_iu_worm_info, warning_if_no_file)

    all_files = list(file_iter)
    if len(all_files) == 0:
        raise Exception(
            "No data for IUs found - see above warnings and check input directory"
        )

    other_worm_directories = worm_directories[1:]
    other_worms = [
        get_sch_worm_info(next(get_sch_flat(f"{input_dir}{other_worm_dir}")).file_path)
        for other_worm_dir in other_worm_directories
    ]

    all_other_worms_file_paths = set(
        file_desc.file_path
        for worm_files_list in (
            get_sch_flat(f"{input_dir}{worm_dir}")
            for worm_dir in other_worm_directories
        )
        for file_desc in worm_files_list
    )

    for file_info in tqdm(all_files, desc="Canoncialise SCH results"):
        other_worm_file_infos = []
        for worm, burden, _, _ in other_worms:
            # folder names are different format than the file name
            worm_burden = "sch-" + worm + "-" + burden.replace("_", "-")
            new_file = swap_worm_in_heirachy(file_info, first_worm, worm_burden)

            if new_file.file_path in all_other_worms_file_paths:
                other_worm_file_infos.append(new_file)

        canonical_result_first_worm = canoncialise_single_result(file_info)

        other_worms_canoncial = [
            canoncialise_single_result(other_worm_file_info, warning_if_no_file)
            for other_worm_file_info in other_worm_file_infos
        ]

        all_worms_canonical = combine_many_worms(
            canonical_result_first_worm, other_worms_canoncial,
            combination_function=probability_any_worm.max_of_any
        )
        output_directory_structure.write_canonical(
            output_dir, file_info, all_worms_canonical
        )


def run_sth_postprocessing_pipeline(
    input_dir: str,
    output_dir: str,
    worm_directories: list[str],
    worm_combination_algorithm: probability_any_worm.WormCombinationFunction,
    num_jobs: int,
    skip_canonical=False,
    threshold: float = 0.1,
    run_country_level_summaries = False,
    warning_if_no_file = False,
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

    Note this will be looking at prevalence across any worm specified in the worm_directories

    """
    if not skip_canonical:
        canonicalise_raw_sth_results(
            input_dir,
            output_dir,
            worm_directories,
            warning_if_no_file,
            worm_combination_algorithm)

    config = PipelineConfig(
        disease=Disease.STH,
        threshold=threshold,
        include_country_and_continent_summaries=run_country_level_summaries,
    )
    pipeline.pipeline(input_dir, output_dir, config)


def run_sch_postprocessing_pipeline(
    input_dir,
    output_dir,
    worm_directories,
    skip_canonical=False,
    threshold: float = 0.1,
    run_country_level_summaries = False,
    warning_if_no_file = False,
):
    if not skip_canonical:
        canonicalise_raw_sch_results(
            input_dir, output_dir, worm_directories, warning_if_no_file
        )
    config = PipelineConfig(
        disease=Disease.SCH,
        threshold=threshold,
        include_country_and_continent_summaries=run_country_level_summaries,
    )
    pipeline.pipeline(input_dir, output_dir, config)


if __name__ == "__main__":
    thresholds_to_process = [0.01, 0.1]
    #     input_dir = "local_data/sth-fresh-backup"
    #     worm_directories = next(os.walk(input_dir))[1]
    #     for worm_directory in worm_directories:
    #         run_sth_postprocessing_pipeline(
    #             input_dir,
    #             f"local_data/sth-output-single-worm/{worm_directory}",
    #             [worm_directory],
    #             1,
    #             skip_canonical=worm_directory == "sth-202410a-roundworm-flat-ntdmc-only",
    #         )
    #     run_sth_postprocessing_pipeline(
    #         input_dir,
    #         "local_data/sth-output-all-worm/",
    #         worm_directories,
    #         1,
    #         skip_canonical=False,
    #     )

    root_input_dir = "local_data/202410b-SCH-test-2-20241022"
    worm_directories = ["sch-haematobium", "sch-mansoni-high-burden", "sch-mansoni-low-burden"]
    for threshold in thresholds_to_process:
        for worm_directory in worm_directories:
            run_sch_postprocessing_pipeline(
                f"{root_input_dir}/",
                f"local_data/sch-output-single-worm/threshold_{threshold}/{worm_directory}",
                skip_canonical=False,
                worm_directories=[worm_directory],
                threshold=threshold,
                run_country_level_summaries=True
            )
        run_sch_postprocessing_pipeline(
            f"{root_input_dir}/",
            f"local_data/sch-output-all-worm/threshold_{threshold}/",
            skip_canonical=False,
            worm_directories=worm_directories,
            threshold=threshold,
            run_country_level_summaries=True
        )
