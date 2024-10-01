from endgame_postprocessing.post_processing import (
    canoncical_columns,
    canonicalise,
    iu_aggregates,
)
from endgame_postprocessing.post_processing.aggregation import (
    add_scenario_and_country_to_raw_data,
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


def canonicalise_raw_oncho_results(input_dir, output_dir):
    file_iter = post_process_file_generator(
        file_directory=input_dir, end_of_file="-raw_all_age_data.csv"
    )

    all_files = list(file_iter)

    for file_info in tqdm(all_files, desc="Canoncialise oncho results"):
        raw_iu = pd.read_csv(file_info.file_path)
        raw_iu.drop(columns=["age_start", "age_end"])
        canonical_result = canonicalise.canonicalise_raw(
            raw_iu, file_info, "prevalence"
        )
        canonicalise.write_canonical(output_dir, file_info, canonical_result)


def iu_statistical_aggregates(working_directory):
    file_iter = post_process_file_generator(
        file_directory=canonicalise.get_canonical_dir(working_directory),
        end_of_file="_canonical.csv",
    )
    with tqdm(total=1, desc="Post-processing Scenarios") as pbar:
        for file_info in file_iter:
            iu_statistical_aggregate = process_single_file(
                raw_model_outputs=pd.read_csv(file_info.file_path),
                scenario=file_info.scenario,
                iuName=file_info.iu,
                prevalence_marker_name=canoncical_columns.PROCESSED_PREVALENCE,
                post_processing_start_time=1970,
                measure_summary_map={
                    canoncical_columns.PROCESSED_PREVALENCE: measure_summary_float
                },
                pct_runs_under_threshold=constants.PCT_RUNS_UNDER_THRESHOLD,
            )

            iu_aggregates.write_iu_aggregate(
                working_directory, file_info, iu_statistical_aggregate
            )

            ### Used to add descriptors to raw data files
            # add_scenario_and_country_to_raw_data(
            #     pd.read_csv(file_info.file_path),
            #     file_info.scenario,
            #     file_info.iu
            # ).to_csv(
            #     "post-processed-outputs/oncho_with_scenario_country/" +
            #     file_info.scenario + "_" +
            #     file_info.iu + "_raw_with_descriptors.csv"
            # )

            custom_progress_bar_update(
                pbar, file_info.scenario_index, file_info.total_scenarios
            )


oncho_dir = "local_data/small_by_scenario"

canonicalise_raw_oncho_results(oncho_dir, "local_data/output")
iu_statistical_aggregates(
    "local_data/output",
)

combined_ius = aggregate_post_processed_files("post-processed-outputs/oncho")
aggregated_df = iu_lvl_aggregate(combined_ius)
aggregated_df.to_csv("post-processed-outputs/aggregated/combined-oncho-iu-lvl-agg.csv")
# aggregated_df = pd.read_csv("post-processed-outputs/aggregated/combined-oncho-iu-lvl-agg.csv")

### Used to aggregate all the raw files
# aggregate_post_processed_files("post-processed-outputs/oncho_with_scenario_country/", specific_files="*.csv").to_csv("post-processed-outputs/aggregated/combined-oncho-raw-iu-lvl-agg.csv")
raw_agg_iu = pd.read_csv("post-processed-outputs/aggregated/combined-oncho-raw-iu-lvl-agg.csv")
country_lvl_data = country_lvl_aggregate(
    raw_iu_data=raw_agg_iu,
    processed_iu_lvl_data=aggregated_df,
    general_summary_measure_names=constants.COUNTRY_SUMMARY_COLUMNS,
    general_groupby_cols=constants.COUNTRY_SUMMARY_GROUP_COLUMNS,
    threshold_summary_measure_names=constants.COUNTRY_THRESHOLD_SUMMARY_COLUMNS,
    threshold_groupby_cols=constants.COUNTRY_THRESHOLD_SUMMARY_GROUP_COLUMNS,
    threshold_cols_rename=constants.COUNTRY_THRESHOLD_RENAME_MAP,
    path_to_population_data=""
)
country_lvl_data.to_csv(
    "post-processed-outputs/aggregated/combined-oncho-country-lvl-agg_2.csv"
)
africa_lvl_aggregate(
    country_lvl_data=country_lvl_data,
    measures_to_summarize=constants.AFRICA_SUMMARY_MEASURES,
    columns_to_group_by=constants.AFRICA_LVL_GROUP_COLUMNS,
).to_csv("post-processed-outputs/aggregated/combined-oncho-africa-lvl-agg.csv")
