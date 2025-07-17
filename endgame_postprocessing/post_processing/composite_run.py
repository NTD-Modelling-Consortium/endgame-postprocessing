import itertools
from typing import List, Dict

import numpy as np
import pandas as pd

from endgame_postprocessing.post_processing import canonical_columns
from endgame_postprocessing.post_processing.iu_data import IUData


def _get_priority_populations(ius: List[pd.DataFrame], iu_metadata: IUData):
    """
    Retrieves the priority populations for each Implementation Unit (IU) across multiple years.

    Args:
        ius (list[pd.DataFrame]): A list of DataFrames, each representing an IU, containing IU-specific data.
        iu_metadata (IUData): An IUData object that provides metadata and helper methods for IUs.

    Returns:
        np.ndarray: A 3D numpy array where each element represents the yearly priority population for an IU.
                    Shape is (number of IUs, years, 1).
    """
    populations = []
    num_years = ius[0][canonical_columns.YEAR_ID].nunique()
    
    for iu in ius:
        iu_code = iu[canonical_columns.IU_NAME].iloc[0]        
        pop_iterator = iu_metadata.get_priority_population_for_iu(iu_code)
        populations.append(list(itertools.islice(pop_iterator, num_years)))
    
    return np.array(populations).reshape(len(ius), -1, 1)


def build_composite_run(
        canonical_iu_runs: List[pd.DataFrame],
        iu_data: IUData,
        is_africa=False,
):
    """
    Build a composite run by aggregating disease prevalence data across multiple Implementation Units (IUs).

    This function performs the following mathematical operations:

    Step 1: Extract draws from canonical IUs (3D array)
           
    all_ius_draws:
                        draws (columns)
                     [draw_0, draw_1, ..., draw_n]
           IU_0 ┌─┬─────────────────────────────┐
                │ │ prevalence values...        │ year_0
                │ ├─────────────────────────────┤
                │ │ prevalence values...        │ year_1
                │ ├─────────────────────────────┤
                │ │ ...                         │ ...
                │ └─────────────────────────────┘
           IU_1 ├─┬─────────────────────────────┐
                │ │ prevalence values...        │
                │ ├─────────────────────────────┤
                │ │ prevalence values...        │
                │ └─────────────────────────────┘
           ...  └─────────────────────────────────┘
           
    Shape: (num_IUs, num_years, num_draws)


    Step 2: Get priority populations (3D array with single column)

    populations:
           IU_0 ┌─┐
                │ │ pop_year_0
                │ │ pop_year_1
                │ │ ...
                └─┘
           IU_1 ┌─┐
                │ │ pop_year_0
                │ │ pop_year_1
                └─┘
           ...
           
    Shape: (num_IUs, num_years, 1)


    Step 3: Element-wise multiplication (broadcasting)

    case_numbers_across_ius = all_ius_draws * populations

           IU_0 ┌─┬─────────────────────────────┐
                │ │ cases = prev × pop          │ year_0
                │ ├─────────────────────────────┤
                │ │ cases = prev × pop          │ year_1
                │ └─────────────────────────────┘
           IU_1 ├─┬─────────────────────────────┐
                │ │ cases = prev × pop          │
                │ └─────────────────────────────┘
           ...
           
    Shape: (num_IUs, num_years, num_draws)


    Step 4: Sum across IUs (axis=0)

    case_numbers_in_country = np.sum(..., axis=0)

                ┌─────────────────────────────┐
                │ Σ(IU_0 + IU_1 + ... IU_n)   │ year_0
                ├─────────────────────────────┤
                │ Σ(IU_0 + IU_1 + ... IU_n)   │ year_1
                ├─────────────────────────────┤
                │ ...                         │ ...
                └─────────────────────────────┘
                
    Shape: (num_years, num_draws)


    Step 5: Divide by total population

    prevalence = case_numbers_in_country / total_population

    total_population:     Final prevalence:
    ┌─────────┐          ┌─────────────────────────────┐
    │ pop_y0  │          │ total_cases/total_pop       │ year_0
    │ pop_y1  │    →     │ total_cases/total_pop       │ year_1
    │ ...     │          │ ...                         │ ...
    └─────────┘          └─────────────────────────────┘

    Shape: (years, 1)     Shape: (num_years, num_draws)

    Args:
        canonical_iu_runs: List of DataFrames containing prevalence data for each IU
        iu_data: IUData object containing population metadata
        is_africa: Whether to compute for Africa (True) or country level (False)

    Returns:
        pd.DataFrame: Composite prevalence data aggregated across all IUs
    """
    # Assumptions: same number of draws in each IU run
    # Same year IDs in each one
    draw_column_names, all_ius_draws = canonical_columns.extract_draws(canonical_iu_runs)

    # Compute the mean number of disease cases as a proportion of the population
    # in each draw, for every IU
    # List[DataFrame] - Each row, of every IU dataframe, corresponds to the number
    # of cases, in that year, across all the draws (columns)
    priority_populations = _get_priority_populations(canonical_iu_runs, iu_data)
    iu_case_numbers = all_ius_draws * priority_populations

    # DataFrame - Sum up the total number of cases from all the IUs
    summed_case_numbers = np.sum(iu_case_numbers, axis=0)

    if is_africa:
        total_population = iu_data.get_priority_population_for_africa()
    else:
        total_population = iu_data.get_priority_population_for_country(
            canonical_iu_runs[0][canonical_columns.COUNTRY_CODE].iloc[0]
        )

    if type(total_population) is dict:
        total_population = np.array(list(total_population.values())).reshape((len(total_population), -1))

    # DataFrame - Mean prevalence (across all IUs) for all the years
    prevalence = pd.DataFrame(
        summed_case_numbers / total_population, columns=draw_column_names
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
