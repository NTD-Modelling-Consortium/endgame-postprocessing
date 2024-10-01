from collections import defaultdict
import itertools
from endgame_postprocessing.post_processing import (
    canoncical_columns,
    canonicalise,
    composite_run,
    output_directory_structure,
)
from endgame_postprocessing.post_processing.aggregation import (
    aggregate_post_processed_files,
    single_country_aggregate,
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
        output_directory_structure.write_canonical(
            output_dir, file_info, canonical_result
        )


def iu_statistical_aggregates(working_directory):
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
                prevalence_marker_name=canoncical_columns.PROCESSED_PREVALENCE,
                post_processing_start_time=1970,
                measure_summary_map={
                    canoncical_columns.PROCESSED_PREVALENCE: measure_summary_float
                },
                pct_runs_under_threshold=constants.PCT_RUNS_UNDER_THRESHOLD,
            )

            output_directory_structure.write_iu_stat_agg(
                working_directory, file_info, iu_statistical_aggregate
            )

            custom_progress_bar_update(
                pbar, file_info.scenario_index, file_info.total_scenarios
            )


def country_composite(working_directory):
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
        country_composite = composite_run.build_composite_run_multiple_scenarios(
            list(
                [
                    pd.read_csv(iu_for_country.file_path)
                    for iu_for_country in ius_for_country
                ]
            ),
            # TODO: provide the population data!
            population_data={},
        )
        output_directory_structure.write_country_composite(
            working_directory, country, country_composite
        )
        yield country_composite


def africa_composite(working_directory):
    canonical_file_iter = post_process_file_generator(
        file_directory=output_directory_structure.get_canonical_dir(working_directory),
        end_of_file="_canonical.csv",
    )

    canonical_ius = list(canonical_file_iter)
    africa_composite = composite_run.build_composite_run_multiple_scenarios(
        list(
            tqdm(
                [pd.read_csv(iu_in_africa.file_path) for iu_in_africa in canonical_ius],
                desc="Building Africa composite run",
            )
        ),
        # TODO: provide the population data!
        population_data={},
    )
    # Currently the composite thing sticks a column for country based on the first IU which
    # isn't required for Africa, but this isn't a nice place to do this!
    africa_composite.drop(columns=[canoncical_columns.COUNTRY_CODE], inplace=True)
    output_directory_structure.write_africa_composite(
        working_directory, africa_composite
    )


def country_aggregate(country_composite):
    country_statistical_aggregates = single_country_aggregate(country_composite)
    # TODO: append the additional measures here
    return country_statistical_aggregates


oncho_dir = "local_data/oncho"
working_directory = "local_data/output"

canonicalise_raw_oncho_results(oncho_dir, "local_data/output")
iu_statistical_aggregates(
    "local_data/output",
)

country_aggregates = [
    country_aggregate(country_composite)
    for country_composite in country_composite(working_directory)
]


all_country_aggregates = pd.concat(country_aggregates)
output_directory_structure.write_country_stat_agg(
    working_directory, all_country_aggregates
)

africa_composite(working_directory)
