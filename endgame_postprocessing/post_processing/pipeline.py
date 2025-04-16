import itertools
from collections import defaultdict

import pandas as pd
from tqdm import tqdm

import endgame_postprocessing.model_wrappers.constants as constants
from endgame_postprocessing.post_processing import (
    composite_run,
    iu_data_fixup,
    output_directory_structure,
    canonical_columns,
)
from endgame_postprocessing.post_processing.aggregation import (
    africa_lvl_aggregate,
    aggregate_post_processed_files,
    single_country_aggregate,
    africa_composite,
    filter_to_maximum_year_range_for_all_ius,
)
from endgame_postprocessing.post_processing.aggregation import (
    iu_lvl_aggregate,
    country_lvl_aggregate,
)
from endgame_postprocessing.post_processing.file_util import (
    post_process_file_generator,
    custom_progress_bar_update,
)
from endgame_postprocessing.post_processing.iu_data import IUData, IUSelectionCriteria
from endgame_postprocessing.post_processing.pipeline_config import PipelineConfig
from endgame_postprocessing.post_processing.single_file_post_processing import (
    process_single_file,
    measure_summary_float,
)


def iu_statistical_aggregates(working_directory, threshold):
    file_iter = post_process_file_generator(
        file_directory=output_directory_structure.get_canonical_dir(working_directory),
        end_of_file="_canonical.csv",
    )
    with tqdm(total=1, desc="Post-processing Scenarios") as pbar:
        for file_info in file_iter:
            iu_statistical_aggregate = process_single_file(
                raw_model_outputs=pd.read_csv(file_info.file_path),
                scenario=file_info.scenario,
                iuName=file_info.iu,
                prevalence_marker_name=canonical_columns.PROCESSED_PREVALENCE,
                post_processing_start_time=1970,
                post_processing_end_time=2041,
                threshold=threshold,
                measure_summary_map={canonical_columns.PROCESSED_PREVALENCE: measure_summary_float},
                pct_runs_under_threshold=constants.PCT_RUNS_UNDER_THRESHOLD,
            )

            output_directory_structure.write_iu_stat_agg(
                working_directory, file_info, iu_statistical_aggregate
            )

            custom_progress_bar_update(pbar, file_info.scenario_index, file_info.total_scenarios)


def country_composite(
    working_directory,
    iu_meta_data,
):
    canonical_file_iter = post_process_file_generator(
        file_directory=output_directory_structure.get_canonical_dir(working_directory),
        end_of_file="_canonical.csv",
    )

    canonical_ius = list(canonical_file_iter)

    canoncial_ius_by_country_iter = itertools.groupby(
        canonical_ius, lambda file_info: file_info.country
    )

    canonical_ius_by_country = defaultdict(list)

    for country, file_info_for_canonical_iu in canoncial_ius_by_country_iter:
        canonical_ius_by_country[country] += file_info_for_canonical_iu

    for country, ius_for_country in tqdm(
        canonical_ius_by_country.items(), desc="Building country composites"
    ):
        cannonical_iu_data_for_country_composite = filter_to_maximum_year_range_for_all_ius(
            list(
                [pd.read_csv(iu_for_country.file_path) for iu_for_country in ius_for_country]
            ),
            keep_na_year_id=False
        )
        country_composite = composite_run.build_composite_run_multiple_scenarios(
            cannonical_iu_data_for_country_composite,
            iu_meta_data,
        )
        output_directory_structure.write_country_composite(
            working_directory, country, country_composite
        )
        yield country_composite


def country_aggregate(
    country_composite: pd.DataFrame,
    iu_lvl_data: pd.DataFrame,
    country_code: str,
    iu_meta_data: IUData,
):
    country_statistical_aggregates = single_country_aggregate(country_composite)
    country_iu_summary_aggregates = country_lvl_aggregate(
        iu_lvl_data,
        constants.COUNTRY_THRESHOLD_SUMMARY_COLUMNS,
        constants.COUNTRY_THRESHOLD_SUMMARY_GROUP_COLUMNS,
        constants.COUNTRY_THRESHOLD_RENAME_MAP,
        constants.PCT_RUNS_UNDER_THRESHOLD,
        iu_meta_data.get_total_ius_in_country(country_code),
    )
    return pd.concat([country_statistical_aggregates, country_iu_summary_aggregates])


def pipeline(input_dir, working_directory, pipeline_config: PipelineConfig):
    iu_statistical_aggregates(working_directory, threshold=pipeline_config.threshold)

    all_ius = set(
        [
            file_info.iu
            for file_info in post_process_file_generator(
                file_directory=output_directory_structure.get_canonical_dir(working_directory),
                end_of_file="_canonical.csv",
            )
        ]
    )

    fixedup_meta_data_file = iu_data_fixup.fixup_iu_meta_data_file(
        pd.read_csv(f"{input_dir}/PopulationMetadatafile.csv"),
        simulated_IUs=all_ius,
    )

    output_directory_structure.write_meta_data_file(working_directory, fixedup_meta_data_file)

    iu_meta_data = IUData(
        fixedup_meta_data_file,
        pipeline_config.disease,
        iu_selection_criteria=IUSelectionCriteria.SIMULATED_IUS,
        simulated_IUs=all_ius,
    )

    all_iu_data = (
        iu_lvl_aggregate(aggregate_post_processed_files(f"{working_directory}/ius/"))
        .sort_values(["scenario", "country_code", "iu_name", "year_id"])
        .reset_index(drop=True)
        .convert_dtypes()  # attempt to reconstruct the types (TODO: why are they lost)
    )

    output_directory_structure.write_combined_iu_stat_agg(
        working_directory, all_iu_data, pipeline_config.disease
    )

    if not pipeline_config.include_country_and_continent_summaries:
        return

    country_aggregates = [
        country_aggregate(
            country_composite,
            # The function returns a list, since the compiled iu aggregates already exist in a
            # single data frame, its passed in as the sole item in the list
            # and it will be the sole item returned
            # The compiled iu aggregate dataframe is used because it contains newly calculated
            # metrics needed for country level statistics.
            filter_to_maximum_year_range_for_all_ius(
                [all_iu_data[
                    (all_iu_data["country_code"] == country_composite["country_code"].values[0])
                ]],
                keep_na_year_id=True
            )[0],
            country_composite["country_code"].values[0],
            iu_meta_data,
        )
        for country_composite in country_composite(working_directory, iu_meta_data)
    ]

    all_country_aggregates = (
        pd.concat(country_aggregates)
        .sort_values(["scenario", "country_code", "year_id"])
        .reset_index(drop=True)
        .convert_dtypes()  # attempt to reconstruct the types (TODO: why are they lost)
    )
    output_directory_structure.write_country_stat_agg(
        working_directory, all_country_aggregates, pipeline_config.disease
    )

    africa_aggregates = (
        africa_lvl_aggregate(
            *africa_composite(
                working_directory, iu_meta_data,
            ),
            prevalence_threshold=pipeline_config.threshold,
            pct_runs_threshold=[0.9, 1.0],
        )
        .sort_values(["scenario", "year_id"])
        .reset_index(drop=True)
        .convert_dtypes()  # attempt to reconstruct the types (TODO: why are they lost)
    )
    output_directory_structure.write_africa_stat_agg(
        working_directory, africa_aggregates, pipeline_config.disease
    )
