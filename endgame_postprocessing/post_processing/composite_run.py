import itertools
from typing import List

import numpy as np
import pandas as pd

from endgame_postprocessing.post_processing import canonical_columns
from endgame_postprocessing.post_processing.iu_data import IUData


def build_iu_case_numbers(canonical_iu_run, population) -> pd.DataFrame:
    return canonical_iu_run.loc[:, "draw_0":] * population


def _get_priority_populations(ius, iu_metadata: IUData):
    populations = [
        iu_metadata.get_priority_population_for_IU(
            iu[canonical_columns.IU_NAME].iloc[0]
        )
        for iu in ius
    ]
    return np.array(populations)[:, np.newaxis, np.newaxis]


def build_composite_run(
        canonical_iu_runs: List[pd.DataFrame],
        iu_data: IUData,
        is_africa=False,
):
    # Assumptions: same number of draws in each IU run
    # Same year IDs in each one
    draw_columns, all_ius_draws = canonical_columns.extract_draws(canonical_iu_runs)

    # Compute the mean number of disease cases as a proportion of the population
    # in each draw, for every IU
    # List[DataFrame] - Each row, of every IU dataframe, corresponds to the number
    # of cases, in that year, across all the draws (columns)
    iu_case_numbers = all_ius_draws * _get_priority_populations(
        canonical_iu_runs, iu_data
    )

    # DataFrame - Sum up the total number of cases from all the IUs
    summed_case_numbers = np.sum(iu_case_numbers, axis=0)

    if is_africa:
        total_population = iu_data.get_priority_population_for_africa()
    else:
        total_population = iu_data.get_priority_population_for_country(
            canonical_iu_runs[0][canonical_columns.COUNTRY_CODE].iloc[0]
        )

    # DataFrame - Mean prevalence (across all IUs) for all the years
    prevalence = pd.DataFrame(
        summed_case_numbers / total_population, columns=draw_columns
    )

    columns_to_use = [
        canonical_columns.YEAR_ID,
        canonical_columns.SCENARIO,
        canonical_columns.COUNTRY_CODE,
        canonical_columns.MEASURE,
    ]

    if is_africa:
        columns_to_use.remove(canonical_columns.COUNTRY_CODE)

    return pd.concat(
        [
            canonical_iu_runs[0][columns_to_use],
            prevalence,
        ],
        axis=1,
    )


def build_composite_run_multiple_scenarios(
        canonical_iu_runs: list[pd.DataFrame],
        iu_data: IUData,
        is_africa=False,
):
    ius_by_scenario = itertools.groupby(
        canonical_iu_runs, lambda run: run["scenario"].iloc[0]
    )

    scenario_results = [
        build_composite_run(list(ius), iu_data, is_africa)
        for _, ius in ius_by_scenario
    ]
    return pd.concat(scenario_results, ignore_index=True)
