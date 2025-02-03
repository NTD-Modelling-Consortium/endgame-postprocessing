import itertools

import numpy as np
import pandas as pd

from endgame_postprocessing.post_processing import canonical_columns
from endgame_postprocessing.post_processing.iu_data import IUData
from endgame_postprocessing.post_processing.measures import calc_prob_under_threshold


def build_iu_case_numbers(canonical_iu_run, population) -> pd.DataFrame:
    return canonical_iu_run.loc[:, "draw_0":] * population


def _get_priority_populations(ius, iu_metadata: IUData):
    populations = [iu_metadata.get_priority_population_for_IU(iu[canonical_columns.IU_NAME].iloc[0])
                   for iu in ius]
    return np.array(populations)[:, np.newaxis, np.newaxis]


def build_composite_run(
        canonical_iu_runs: list[pd.DataFrame],
        iu_data: IUData,
        prevalence_threshold: float = 0.01,
        is_africa=False
):
    # Assumptions: same number of draws in each IU run
    # Same year IDs in each one
    draw_columns = canonical_iu_runs[0].loc[:, "draw_0":].columns
    all_ius_draws = np.array([iu_run[draw_columns].to_numpy() for iu_run in canonical_iu_runs])

    # Compute the mean number of disease cases as a proportion of the population in each draw, for every IU
    # List[DataFrame] - Each row, of every IU dataframe, corresponds to the number of cases, in that year, across all the draws (columns)
    iu_case_numbers = all_ius_draws * _get_priority_populations(canonical_iu_runs, iu_data)

    # DataFrame - Sum up the total number of cases from all the IUs
    summed_case_numbers = np.sum(iu_case_numbers, axis=0)

    if is_africa:
        total_population = iu_data.get_priority_population_for_africa()
    else:
        total_population = iu_data.get_priority_population_for_country(
            canonical_iu_runs[0][canonical_columns.COUNTRY_CODE].iloc[0]
        )

    # DataFrame - Mean prevalence (across all IUs) for all the years
    prevalence = pd.DataFrame(summed_case_numbers / total_population,
                              columns=draw_columns)

    columns_to_use = [
        canonical_columns.YEAR_ID,
        canonical_columns.SCENARIO,
        canonical_columns.COUNTRY_CODE,
        canonical_columns.MEASURE,
    ]

    if is_africa:
        columns_to_use.remove(canonical_columns.COUNTRY_CODE)

    composite_df = pd.concat(
        [
            canonical_iu_runs[0][
                columns_to_use
            ],
            prevalence,
        ],
        axis=1,
    )

    if is_africa:
        """
        We want to compute two new metrics
            1. Probability that all IUs are below threshold - `prob_all_ius_under_threshold`
            2. Proportion of IUs in 100% of runs that are below threshold - `pct_of_ius_with_100pct_runs_under_threshold`

        Computing `prob_all_ius_under_threshold` requires -
            1. for every IU, we associate a boolean array indicating whether the prevalence in a draw
                in a given year is under threshold.
            2. for every year and draw, whether all IUs are under threshold. This will create the second boolean array.
            3. for every year, compute the proportion of `true` values. (calc_prob_under_threshold) can be used
             for this purpose.
        """
        # 1. Compute the boolean mask indicating the draws under threshold across all IUs => PxMxN shaped boolean array
        all_ius_under_threshold = np.all(all_ius_draws <= prevalence_threshold, axis=0)

        # TODO(CA, 3.2.2025): Compute the proportions in a different function?
        # 2. Check if all IUs are under threshold by collapsing along the first dimension using the AND operation => MxN shaped boolean array
        prob_all_ius_under_threshold = calc_prob_under_threshold(all_ius_under_threshold, prevalence_threshold)
        prob_all_ius_under_threshold_df = pd.DataFrame(prob_all_ius_under_threshold)
        prob_all_ius_under_threshold_df = pd.concat(
            [
                canonical_iu_runs[0][
                    [canonical_columns.YEAR_ID, canonical_columns.SCENARIO, canonical_columns.MEASURE]
                ],
                prob_all_ius_under_threshold_df,
            ],
            axis=1,
        )
        prob_all_ius_under_threshold_df[canonical_columns.MEASURE] = "prob_all_ius_under_threshold"
        composite_df = pd.concat([composite_df, prob_all_ius_under_threshold_df], axis=0)

    return composite_df


def build_composite_run_multiple_scenarios(canonical_iu_runs: list[pd.DataFrame],
                                           iu_data: IUData,
                                           prevalence_threshold: float = 0.01,
                                           is_africa=False):
    ius_by_scenario = itertools.groupby(
        canonical_iu_runs, lambda run: run["scenario"].iloc[0]
    )

    scenario_results = [
        build_composite_run(list(ius),
                            iu_data,
                            prevalence_threshold,
                            is_africa) for _, ius in ius_by_scenario
    ]
    return pd.concat(scenario_results, ignore_index=True)
