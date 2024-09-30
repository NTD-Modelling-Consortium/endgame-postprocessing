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
    return raw.query("measure == 'prevalence'").reindex()


def build_composite(input_dir):
    lf_dir = "input-data/lf/"

    file_iter = post_process_file_generator(
        file_directory=input_dir, end_of_file="-raw_all_age_data.csv"
    )
    all_files = [file for file in file_iter]
    total_ius = len(all_files)

    raw_ius = list(
        [
            canonical(pd.read_csv(file_info.file_path), file_info)
            for file_info in tqdm(all_files, desc="Post-processing Scenarios")
        ]
    )

    composite_run.build_composite_run(raw_ius, {}).to_csv("composite.csv")


build_composite("local_data/oncho")
