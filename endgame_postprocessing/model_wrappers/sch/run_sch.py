from functools import reduce
from operator import mul
import os
from tqdm import tqdm
from endgame_postprocessing.post_processing import (
    canonicalise,
    output_directory_structure,
)
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
    raw_without_columns = raw_iu.drop(columns=["espen_loc"])
    # TODO: canonical shouldn't need the age_start / age_end but these are assumed present later
    return canonicalise.canonicalise_raw(
        raw_without_columns, file_info, "true mf prevalence (all pop)"
    )


def combine_many_worms(first_worm, other_worms):
    other_worm_draws = [other_worm.loc[:, "draw_0":] for other_worm in other_worms]
    first_worm.loc[:, "draw_0":] = probability_any_worm(
        [first_worm.loc[:, "draw_0":]] + other_worm_draws
    )
    return first_worm


def canonicalise_raw_sth_results(input_dir, output_dir):
    worms = next(os.walk("."))[1]

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

        other_worms_canoncial = [
            canoncialise_single_result(other_worm_path)
            for other_worm_path in other_worm_paths
        ]

        all_worms_canonical = combine_many_worms(
            canonical_result_first_worm, other_worms_canoncial
        )
        output_directory_structure.write_canonical(
            output_dir, file_info, all_worms_canonical
        )
