from post_processing.aggregation import (
    iu_lvl_aggregate,
    country_lvl_aggregate,
    africa_lvl_aggregate,
)
from post_processing.single_file_post_processing import (
    process_single_file,
    measure_summary_float,
)
from post_processing.file_util import (
    post_process_file_generator,
    custom_progress_bar_update,
)
import model_wrappers.constants as constants
from tqdm import tqdm
import pandas as pd


oncho_dir = "input-data/oncho/"
file_iter = post_process_file_generator(
    file_directory=oncho_dir, end_of_file="-raw_all_age_data.csv"
)
with tqdm(total=1, desc="Post-processing Scenarios") as pbar:
    for file_info in file_iter:
        process_single_file(
            raw_model_outputs=pd.read_csv(file_info.file_path),
            scenario=file_info.scenario,
            iuName=file_info.iu,
            prevalence_marker_name="prevalence",
            post_processing_start_time=1970,
            measure_summary_map={"prevalence": measure_summary_float},
        ).to_csv(
            "post-processed-outputs/oncho/" + file_info.scenario + "_" +
            file_info.iu + "post_processed.csv"
        )
        custom_progress_bar_update(pbar, file_info.scenario_index, file_info.total_scenarios)

aggregated_df = iu_lvl_aggregate(
    "post-processed-outputs/oncho",
)
aggregated_df.to_csv("post-processed-outputs/aggregated/combined-oncho-iu-lvl-agg.csv")
country_lvl_data = country_lvl_aggregate(
    iu_lvl_data=aggregated_df,
    measure_column_name=constants.MEASURE_COLUMN_NAME,
    general_summary_cols=constants.COUNTRY_SUMMARY_COLUMNS,
    general_groupby_cols=constants.COUNTRY_SUMMARY_GROUP_COLUMNS,
    threshold_summary_cols=constants.COUNTRY_THRESHOLD_SUMMARY_COLUMNS,
    threshold_groupby_cols=constants.COUNTRY_THRESHOLD_SUMMARY_GROUP_COLUMNS,
    threshold_cols_rename=constants.COUNTRY_THRESHOLD_RENAME_MAP,
)
country_lvl_data.to_csv(
    "post-processed-outputs/aggregated/combined-oncho-country-lvl-agg.csv"
)
africa_lvl_aggregate(
    country_lvl_data=country_lvl_data,
    measure_column_name=constants.MEASURE_COLUMN_NAME,
    measures_to_summarize=constants.AFRICA_SUMMARY_MEASURES,
    columns_to_group_by=constants.AFRICA_LVL_GROUP_COLUMNS,
).to_csv("post-processed-outputs/aggregated/combined-oncho-africa-lvl-agg.csv")
