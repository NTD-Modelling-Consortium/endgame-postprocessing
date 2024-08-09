from endgame_postprocessing.post_processing.aggregation import (
    aggregate_post_processed_files,
)
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


lf_dir = "input-data/lf/"
file_iter = post_process_file_generator(
    file_directory=lf_dir, end_of_file=".csv"
)
with tqdm(total=1, desc="Post-processing Scenarios") as pbar:
    for file_info in file_iter:
        process_single_file(
            raw_model_outputs=pd.read_csv(file_info.file_path),
            scenario=file_info.scenario,
            iuName=file_info.iu,
            prevalence_marker_name="sampled mf prevalence (all pop)",
            post_processing_start_time=1970,
            measure_summary_map={"sampled mf prevalence (all pop)": measure_summary_float,
                                 "true mf prevalence (all pop)": measure_summary_float},
        ).to_csv(
            "post-processed-outputs/lf/" + file_info.scenario + "_" +
            file_info.iu + "post_processed.csv"
        )
        custom_progress_bar_update(pbar, file_info.scenario_index, file_info.total_scenarios)


combined_ius = aggregate_post_processed_files("post-processed-outputs/lf")
aggregated_df = iu_lvl_aggregate(combined_ius)
aggregated_df.to_csv("post-processed-outputs/aggregated/combined-lf-iu-lvl-agg.csv")
country_lvl_data = country_lvl_aggregate(
    iu_lvl_data=aggregated_df,
    measure_column_name=constants.MEASURE_COLUMN_NAME,
    general_summary_measure_names=[
        "sampled mf prevalence (all pop)",
        "year_of_threshold_prevalence_avg",
        "year_of_90_under_threshold",
    ],
    general_groupby_cols=constants.COUNTRY_SUMMARY_GROUP_COLUMNS,
    threshold_summary_cols=[
        "year_of_threshold_prevalence_avg",
        "year_of_90_under_threshold",
    ],
    threshold_groupby_cols=constants.COUNTRY_THRESHOLD_SUMMARY_GROUP_COLUMNS,
    threshold_cols_rename=constants.COUNTRY_THRESHOLD_RENAME_MAP,
)
country_lvl_data.to_csv(
    "post-processed-outputs/aggregated/combined-lf-country-lvl-agg.csv"
)
africa_lvl_aggregate(
    country_lvl_data=country_lvl_data,
    measure_column_name=constants.MEASURE_COLUMN_NAME,
    measures_to_summarize=["sampled mf prevalence (all pop)"],
    columns_to_group_by=constants.AFRICA_LVL_GROUP_COLUMNS,
).to_csv("post-processed-outputs/aggregated/combined-lf-africa-lvl-agg.csv")
