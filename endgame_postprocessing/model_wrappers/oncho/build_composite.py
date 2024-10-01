from collections import defaultdict
import itertools
from endgame_postprocessing.post_processing.aggregation import (
    aggregate_post_processed_files,
)
from endgame_postprocessing.post_processing.aggregation import (
    iu_lvl_aggregate,
    country_lvl_aggregate,
    africa_lvl_aggregate,
)
from endgame_postprocessing.post_processing.single_file_post_processing import (
    process_single_file,
    measure_summary_float,
)
from endgame_postprocessing.post_processing.file_util import (
    post_process_file_generator,
    custom_progress_bar_update,
)
import endgame_postprocessing.model_wrappers.constants as constants
from tqdm import tqdm
import pandas as pd

from endgame_postprocessing.post_processing import composite_run


def canonical(raw, file_info):
    raw.insert(1, "iu_code", file_info.iu)
    raw.insert(2, "scenario", file_info.scenario)
    return raw.query("measure == 'prevalence'").reset_index()


def group_by_key(file_info):
    return file_info.country


def build_composite(input_dir):
    lf_dir = "input-data/lf/"

    file_iter = post_process_file_generator(
        file_directory=input_dir, end_of_file="-raw_all_age_data.csv"
    )
    all_files = list([file for file in file_iter])
    total_ius = len(all_files)

    tqdm_file_iterator = tqdm(all_files, total=total_ius, desc="All files")

    canoncial_ius_by_country = [
        (
            country,
            [
                canonical(pd.read_csv(file_info.file_path), file_info)
                for file_info in file_infos
            ],
        )
        for country, file_infos in itertools.groupby(tqdm_file_iterator, group_by_key)
    ]

    gathered_ius = defaultdict(list)

    for country, ius_for_country in canoncial_ius_by_country:
        gathered_ius[country] += ius_for_country

    for country, ius_for_country in tqdm(gathered_ius.items(), desc="Summarising"):
        composite_run.build_composite_run_multiple_scenarios(
            ius_for_country, {}
        ).to_csv(f"country-agg-{country}.csv")


build_composite("local_data/oncho")
