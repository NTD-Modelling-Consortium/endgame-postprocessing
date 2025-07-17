import itertools
from typing import List, Dict, Tuple, Optional

import numpy as np
import pandas as pd

from endgame_postprocessing.post_processing import canonical_columns
from endgame_postprocessing.post_processing.iu_data import IUData


def _compute_year_range_intersection(canonical_iu_runs: List[pd.DataFrame], iu_data: IUData) -> Optional[Tuple[int, int]]:
    """
    Compute the intersection of simulation years and metadata years.
    
    Args:
        canonical_iu_runs (List[pd.DataFrame]): List of simulation IU DataFrames
        iu_data (IUData): Population metadata object
        
    Returns:
        Optional[Tuple[int, int]]: (min_year, max_year) intersection, or None if no intersection
        
    Example:
        ```python
        # Simulation years: 2000-2025, Metadata years: 2010-2030
        intersection = _compute_year_range_intersection(canonical_ius, iu_data)
        # Returns: (2010, 2025)
        ```
    """
    if not canonical_iu_runs:
        return None
        
    # Get simulation year range
    simulation_years = set(canonical_iu_runs[0][canonical_columns.YEAR_ID])
    sim_min = min(simulation_years)
    sim_max = max(simulation_years)
    
    # For non-longitudinal data, any simulation year range is valid
    if not iu_data.is_longitudinal:
        return (sim_min, sim_max)
    
    # Get metadata year range
    year_range = iu_data.year_range
    if not year_range:  # Empty dict for non-longitudinal
        return (sim_min, sim_max)
    
    metadata_min = year_range['min']
    metadata_max = year_range['max']
    
    # Compute intersection
    intersection_min = max(sim_min, metadata_min)
    intersection_max = min(sim_max, metadata_max)
    
    # Return None if no valid intersection
    if intersection_min > intersection_max:
        return None
        
    return (intersection_min, intersection_max)


def _trim_canonical_ius_to_year_range(canonical_iu_runs: List[pd.DataFrame], year_range: Tuple[int, int]) -> List[pd.DataFrame]:
    """
    Trim canonical IU runs to only include years within the specified range.
    
    Args:
        canonical_iu_runs (List[pd.DataFrame]): List of simulation IU DataFrames
        year_range (Tuple[int, int]): (min_year, max_year) to include
        
    Returns:
        List[pd.DataFrame]: Trimmed DataFrames containing only years in the range
        
    Example:
        ```python
        # Original data: years 2008-2012, trim to 2010-2012
        trimmed = _trim_canonical_ius_to_year_range(canonical_ius, (2010, 2012))
        # Returns: DataFrames with only years 2010, 2011, 2012
        ```
    """
    min_year, max_year = year_range
    
    trimmed_ius = []
    for iu_df in canonical_iu_runs:
        # Filter to only include years within the range
        year_mask = (
            (iu_df[canonical_columns.YEAR_ID] >= min_year) & 
            (iu_df[canonical_columns.YEAR_ID] <= max_year)
        )
        trimmed_df = iu_df[year_mask].copy().reset_index(drop=True)
        
        # Only include if there are still rows after filtering
        if not trimmed_df.empty:
            trimmed_ius.append(trimmed_df)
    
    return trimmed_ius


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
    # Trim simulation data to match metadata year range for longitudinal data
    year_intersection = _compute_year_range_intersection(canonical_iu_runs, iu_data)
    if year_intersection is None:
        raise ValueError("No overlap between simulation years and population metadata years")
    
    canonical_iu_runs = _trim_canonical_ius_to_year_range(canonical_iu_runs, year_intersection)
    
    if not canonical_iu_runs:
        raise ValueError("No IU data remaining after trimming to metadata year range")
    
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
        total_population_iter = iu_data.get_priority_population_for_africa()
    else:
        total_population_iter = iu_data.get_priority_population_for_country(
            canonical_iu_runs[0][canonical_columns.COUNTRY_CODE].iloc[0]
        )

    # Use islice to get the right number of years for both longitudinal and non-longitudinal data
    num_years = canonical_iu_runs[0][canonical_columns.YEAR_ID].nunique()
    total_population = np.array(list(itertools.islice(total_population_iter, num_years))).reshape((-1, 1))

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
