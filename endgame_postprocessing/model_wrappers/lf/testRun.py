import os
import shutil
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


def run_aggregate_runs_for_each_iu(input_dir: str, output_iu_dir: str):
    """
    Aggregates the stochastic runs (into mean etc)
    into standard format the input files found in input_dir.
    input_dir must have the following substructure:
        scenario1/
            country1/
                iu1/
                    iu.csv
        scenario2/
    The output directory must be empty.
    On completion the sub-structure will be:
    output_iu_dir/
        a csv per IU with name format
        scenario1_iu1_post_processed.csv
    """
    os.makedirs(output_iu_dir)
    file_iter = post_process_file_generator(
        file_directory=input_dir, end_of_file=".csv"
    )
    with tqdm(total=1, desc="Post-processing Scenarios") as pbar:
        for file_info in file_iter:
            process_single_file(
                raw_model_outputs=pd.read_csv(file_info.file_path),
                scenario=file_info.scenario,
                iuName=file_info.iu,
                prevalence_marker_name="sampled mf prevalence (all pop)",
                post_processing_start_time=1970,
                measure_summary_map={
                    "sampled mf prevalence (all pop)": measure_summary_float,
                    "true mf prevalence (all pop)": measure_summary_float,
                },
            ).to_csv(
                f"{output_iu_dir}/{file_info.scenario}_{file_info.iu}_post_processed.csv"
            )
            custom_progress_bar_update(
                pbar, file_info.scenario_index, file_info.total_scenarios
            )


def generate_aggregates_for_all_ius(processed_iu_dir: str, output_aggregate_dir: str):
    """
    Generates the aggregate files - one for all IUs, one for all countries, one for Africa.
    Expected to be run after run_aggregate_runs_for_each_iu

    processed_iu_dir must have the following substructure:
    processed_iu_dir/
        a csv per IU with name format
        scenario1_iu1_post_processed.csv
        The output directory must be empty.
    On completion the sub-structure will be:
    output_aggregate_dir/
        combined-lf-iu-lvl-agg.csv - all IUs in one csv
            a aggregated by country csv
        combined-lf-country-lvl-agg.csv - aggregate by country
        combined-lf-africa-lvl-agg.csv - aggregated across Africa
    """
    os.makedirs(output_aggregate_dir)
    combined_ius = aggregate_post_processed_files(processed_iu_dir)
    aggregated_df = iu_lvl_aggregate(combined_ius)
    aggregated_df.to_csv(f"{output_aggregate_dir}/combined-lf-iu-lvl-agg.csv")
    country_lvl_data = country_lvl_aggregate(
        iu_lvl_data=aggregated_df,
        general_summary_measure_names=[
            "sampled mf prevalence (all pop)",
            "year_of_threshold_prevalence_avg",
            "year_of_90_under_threshold",
        ],
        general_groupby_cols=constants.COUNTRY_SUMMARY_GROUP_COLUMNS,
        threshold_summary_measure_names=[
            "year_of_threshold_prevalence_avg",
            "year_of_90_under_threshold",
        ],
        threshold_groupby_cols=constants.COUNTRY_THRESHOLD_SUMMARY_GROUP_COLUMNS,
        threshold_cols_rename=constants.COUNTRY_THRESHOLD_RENAME_MAP,
    )
    country_lvl_data.to_csv(f"{output_aggregate_dir}/combined-lf-country-lvl-agg.csv")
    africa_lvl_aggregate(
        country_lvl_data=country_lvl_data,
        measures_to_summarize=["sampled mf prevalence (all pop)"],
        columns_to_group_by=constants.AFRICA_LVL_GROUP_COLUMNS,
    ).to_csv(f"{output_aggregate_dir}/combined-lf-africa-lvl-agg.csv")


def run_postprocessing_pipeline(input_dir: str, output_dir: str):
    """
    Aggregates into standard format the input files found in input_dir.
    input_dir must have the following substructure:
        scenario1/
            country1/
                iu1/
                    iu.csv
        scenario2/

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

    """
    output_iu_dir = f"{output_dir}/ius/"
    run_aggregate_runs_for_each_iu(input_dir, output_iu_dir)
    output_aggregate_dir = f"{output_dir}/aggregated/"
    generate_aggregates_for_all_ius(output_iu_dir, output_aggregate_dir)


if __name__ == "__main__":
    output_dir = "post-processing-outputs/lf"
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
    run_postprocessing_pipeline(input_dir="input-data/lf", output_dir=output_dir)
