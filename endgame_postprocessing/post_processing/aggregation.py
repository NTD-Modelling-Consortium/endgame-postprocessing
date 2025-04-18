import csv
import glob
import itertools
import os
from functools import partial
from pathlib import Path
from typing import List, Tuple, Generator

import numpy as np
import pandas as pd
from tqdm import tqdm

from endgame_postprocessing.post_processing import (
    output_directory_structure,
    composite_run,
    canonical_columns,
)
from endgame_postprocessing.post_processing.measures import measure_summary_float
from .constants import (
    DRAW_COLUMNN_NAME_START,
    MEASURE_COLUMN_NAME,
    PERCENTILES_TO_CALC,
    AGGEGATE_DEFAULT_TYPING_MAP,
    PROB_UNDER_THRESHOLD_MEASURE_NAME,
)
from .file_util import post_process_file_generator
from .iu_data import IUData


def _percentile(n):
    def percentile_(x):
        return np.percentile(x, n)

    percentile_.__name__ = "%s_percentile" % n
    return percentile_


def _is_invalid_year(year):
    return year.isin([-1, pd.NA, np.nan])


def year_all_ius_reach_threshold(years_iu_reach_threshold):
    if len(years_iu_reach_threshold[_is_invalid_year(years_iu_reach_threshold)]) > 0:
        return -1
    return years_iu_reach_threshold.max()


def _calc_count_of_pct_runs(x, pct_of_runs=0, denominator_val=1):
    return len(x[x >= pct_of_runs]) / denominator_val


def _tqdm_unknown_length(generator: Generator, desc: str = "") -> Generator:
    """
    Wraps a generator with tqdm for progress tracking when total length is unknown.
    """
    with tqdm(desc=desc) as progress_bar:
        for item in generator:
            yield item
            progress_bar.update(1)


def add_scenario_and_country_to_raw_data(data, scenario_name, iu_name):
    data["scenario_name"] = scenario_name
    data["country_code"] = iu_name[:3]
    data["iu_name"] = iu_name
    return data


def aggregate_post_processed_files(
    path_to_files: str,
    specific_files: str = "*.csv",
) -> pd.DataFrame:
    """
    Combines all data outputs in a given folder and filters as necessary. The format of the csv's in
    the folder should be the same output format of a call to `process_single_file`

    Args:
        path_to_files (str): the top level folder where the output files are located.
        specific_files (str): file name filter to only combine the files that are wanted.

    Returns:
        A dataframe with all of the CSVs stacked ontop of one another
    """
    rows = []
    columns = []

    # To ensure path ends in a trailing slash
    properly_terminated_path = os.path.join(path_to_files, "")
    files_to_combine = sorted(
        glob.glob(properly_terminated_path + "**/" + specific_files, recursive=True)
    )
    total_files = len(files_to_combine)
    for filename in tqdm(files_to_combine, total=total_files, desc="Processing files"):
        with open(filename, newline="") as f:
            reader = csv.reader(f)
            if len(columns) == 0:
                columns = next(reader)
            else:
                next(reader)
            rows.extend(reader)
    return pd.DataFrame(rows, columns=columns)


def iu_lvl_aggregate(
    aggregated_data: pd.DataFrame,
    columns_to_replace_with_nan: list[str] = ["mean"],
    typing_map: dict = AGGEGATE_DEFAULT_TYPING_MAP,
) -> pd.DataFrame:
    """
    A wrapper function for aggregate_post_processed_files that takes stacked data of all
    the IUs, and returns aggregated iu-lvl data, with columns modified to the correct types, and
    filtered if requested.

    Args:
        aggregated_data: All the IU level data stacked into single DataFrame
                            (See aggregate_post_processed_files)
        columns_to_replace_with_nan (list[str]): any columns that should be replaced with NaN rather
                                                    than None.
        typing_map (dict): a dictionary that maps column names to their dtype.

    Returns:
        A dataframe with all of the iu-lvl data, filtered or not
    """
    # replace empty strings with nan/none where required, and properly type the columns
    aggregated_data.loc[:, columns_to_replace_with_nan] = aggregated_data.loc[
        :, columns_to_replace_with_nan
    ].replace("", np.nan)
    aggregated_data.loc[:, ~aggregated_data.columns.isin(columns_to_replace_with_nan)] = (
        aggregated_data.loc[:, ~aggregated_data.columns.isin(columns_to_replace_with_nan)].replace(
            "", None
        )
    )
    aggregated_data = aggregated_data.astype(typing_map)

    return aggregated_data


def _group_country_pop(data, column_to_group_by):
    grouped_data = data.groupby(["country_code", column_to_group_by]).agg(
        country_pop=("population", "sum")
    )

    return grouped_data[grouped_data[column_to_group_by] is True].copy()


def _threshold_summary_helper(
    data,
    summary_measure_names,
    group_by_columns,
    aggregate_function,
    aggregate_prefix,
    measure_rename_map,
):
    summarize_threshold = (
        data[data[MEASURE_COLUMN_NAME].isin(summary_measure_names)]
        .groupby(group_by_columns)
        .agg({"mean": [("mean", aggregate_function)]})
        .reset_index()
    )
    summarize_threshold.columns = group_by_columns + ["mean"]
    summarize_threshold[MEASURE_COLUMN_NAME] = [
        aggregate_prefix + measure_rename_map[measure_val]
        for measure_val in summarize_threshold[MEASURE_COLUMN_NAME]
        if measure_val in measure_rename_map
    ]
    return summarize_threshold


def _yearly_pct_of_runs_threshold_summary_helper(
    processed_iu_lvl_data, group_by_cols, denominator_to_use, pct_of_runs
):
    summarize_threshold = _threshold_summary_helper(
        processed_iu_lvl_data,
        [PROB_UNDER_THRESHOLD_MEASURE_NAME],
        group_by_cols,
        partial(
            _calc_count_of_pct_runs,
            pct_of_runs=pct_of_runs,
            denominator_val=denominator_to_use,
        ),
        "pct_of_",
        {
            PROB_UNDER_THRESHOLD_MEASURE_NAME: f"ius_with_{int(pct_of_runs * 100)}"
            f"pct_runs_under_threshold"
        },
    )

    summarize_threshold_counts = _threshold_summary_helper(
        processed_iu_lvl_data,
        [PROB_UNDER_THRESHOLD_MEASURE_NAME],
        group_by_cols,
        partial(_calc_count_of_pct_runs, pct_of_runs=pct_of_runs, denominator_val=1),
        "count_of_",
        {
            PROB_UNDER_THRESHOLD_MEASURE_NAME: f"ius_with_{int(pct_of_runs * 100)}"
            f"pct_runs_under_threshold"
        },
    )
    return pd.concat([summarize_threshold, summarize_threshold_counts], axis=0, ignore_index=True)


def _extract_columns_as_numpy_array(dfs: List[pd.DataFrame], columns: List[str]):
    """
    Extract columns from the list of dataframes and stacks them into a 3D array.
    Args:
        dfs (list): List of IU dataframes.
        columns (Index): Columns to extract from each dataframe.
    Returns:
        np.ndarray: A 3D array with shape [P, M, N] where
                    P = Number of dataframes,
                    M = Number of rows,
                    N = Number of columns.
    """
    return np.stack([df[columns].to_numpy() for df in dfs], axis=0)


def _compute_prob_all_ius_below_threshold(mask_below_threshold: np.ndarray) -> np.ndarray:
    """
    Computes the proportion of draws where all IUs are below the threshold for each year.

    Args:
        mask_below_threshold (np.ndarray): A boolean mask indicating if IUs are below the threshold;
                                           shape [P, M, N].

    Returns:
        np.ndarray [M,]: Proportion of draws where all IUs are below the threshold for each year
    """
    # For each year and draw, determine whether all IUs are below threshold
    # Shape: [M,N]
    all_ius_under_threshold = mask_below_threshold.all(axis=0)

    # Compute proportion of TRUE values across draws for each year
    # Shape: [M,]
    return all_ius_under_threshold.mean(axis=1)


def _compute_proportion_ius_with_xpct_runs_under_threshold(
    mask_below_threshold: np.ndarray, pct_runs_threshold_array: np.ndarray
) -> np.ndarray:
    """
    Compute the proportion of IUs with >= x% runs below the threshold.

    Args:
        mask_below_threshold (np.ndarray): Boolean mask indicating draws below the threshold.
                                            Shape: [P, M, N] where P = no. of IUs,
                                            M = no. of years, and N = no. of draws.
        pct_runs_threshold_array (np.ndarray): Array of percentage thresholds.
                                               Shape: [1, 1, len(pct_runs_threshold)].

    Returns:
        np.ndarray: Proportion of IUs meeting thresholds for each year and percentage threshold.
                    Shape: [M, K] where M = no. of years, and K = len(pct_runs_threshold).
    """
    # Compute the proportion of draws below the threshold for each IU
    # Shape: [P, M, 1]
    prop_draws_below_threshold_per_iu = mask_below_threshold.mean(axis=2, keepdims=True)

    # Identify, in every IU, draw proportions above thresholds in `pct_runs_threshold_array`
    # Shape: [P, M, K]
    ius_meet_runs_threshold = prop_draws_below_threshold_per_iu >= pct_runs_threshold_array

    # Compute the proportion of IUs where >= x% draws are under threshold
    # Shape: [M, K]
    return ius_meet_runs_threshold.mean(axis=0)


def _calc_extinction_metrics(
    canonical_iu_dataframes: List[pd.DataFrame],
    extinction_threshold: float = 0.01,
    pct_runs_threshold: List[float] = [0.5, 0.75, 0.9, 1.0],
):
    """
    Computes two metrics for each scenario:
    1. Probability that all IUs are below the threshold in a random draw
       (`prob_all_ius_under_threshold`).
    2. Proportion of IUs where x% of simulations are below the threshold
       (`prop_ius_with_xpct_runs_under_threshold`) where `x` is from `pct_runs_threshold`.

    Args:
        canonical_iu_dataframes: List of DataFrames, each containing IU-level data.
        extinction_threshold: Prevalence threshold below which the disease is considered eliminated.
        pct_runs_threshold: Fraction of runs required to be under the threshold for the Metric 2.

    Returns:
        Dictionary mapping each scenario to a DataFrame containing both metrics for all the years.
    """
    # Group DataFrames by scenario
    canonical_ius_by_scenario = {
        s: list(it)
        for s, it in itertools.groupby(
            canonical_iu_dataframes, lambda run: run[canonical_columns.SCENARIO].iloc[0]
        )
    }

    # Extract relevant columns
    draw_columns = canonical_iu_dataframes[0].loc[:, "draw_0":].columns
    year_column = canonical_iu_dataframes[0][canonical_columns.YEAR_ID]

    # Convert `pct_runs_threshold` into a 3D array for broadcasting
    # Shape: [1, 1, len(pct_runs_threshold)]
    pct_runs_threshold_array = np.array(pct_runs_threshold).reshape(1, 1, -1)

    dfs = {}
    for scenario, ius in canonical_ius_by_scenario.items():
        # 1. Compute Metric 1: Proportion of draws where all IUs are under threshold
        # Compute boolean mask indicating draws below the threshold for all IUs
        # Shape: [P,M,N]
        ius_below_threshold = (
            _extract_columns_as_numpy_array(ius, draw_columns) <= extinction_threshold
        )

        prob_all_ius_under_threshold = _compute_prob_all_ius_below_threshold(ius_below_threshold)

        # Replace the original code block with a call to the helper function
        prop_ius_with_xpct_runs_under_threshold = (
            _compute_proportion_ius_with_xpct_runs_under_threshold(
                ius_below_threshold, pct_runs_threshold_array
            )
        )

        # Create DataFrame for the Metric 1
        df_all_ius = pd.DataFrame(
            {
                canonical_columns.YEAR_ID: year_column,
                canonical_columns.SCENARIO: scenario,
                canonical_columns.MEASURE: "prob_all_ius_under_threshold",
                "mean": prob_all_ius_under_threshold,
            }
        )

        # Create DataFrame for the Metric 2
        df_prop_ius_columns = [
            f"prop_ius_with_{int(x*100)}pct_runs_under_threshold" for x in pct_runs_threshold
        ]

        df_prop_ius = pd.DataFrame(
            prop_ius_with_xpct_runs_under_threshold,
            columns=df_prop_ius_columns,
        )
        df_prop_ius[canonical_columns.YEAR_ID] = year_column
        df_prop_ius[canonical_columns.SCENARIO] = scenario
        df_prop_ius = df_prop_ius.melt(
            id_vars=[canonical_columns.YEAR_ID, canonical_columns.SCENARIO],
            var_name=canonical_columns.MEASURE,
            value_name="mean",
        )

        # Combine results into a single DataFrame
        dfs[scenario] = pd.concat([df_all_ius, df_prop_ius], axis=0)
    return dfs


def aggregate_draws(composite_data: pd.DataFrame) -> pd.DataFrame:
    draw_names = list(
        filter(
            lambda name: name.startswith(DRAW_COLUMNN_NAME_START),
            composite_data.columns,
        )
    )
    statistical_aggregate = measure_summary_float(
        data_to_summarize=composite_data.to_numpy(),
        year_id_loc=composite_data.columns.get_loc(canonical_columns.YEAR_ID),
        measure_column_loc=composite_data.columns.get_loc(canonical_columns.MEASURE),
        # I think we should keep this as the prevalence values have different age starts / age ends
        age_start_loc=0,
        age_end_loc=0,
        draws_loc=[composite_data.columns.get_loc(name) for name in draw_names],
    )
    statistical_aggregate_final = pd.DataFrame(
        statistical_aggregate,
        columns=["year_id", "age_start", "age_end", "measure", "mean"]
        + [f"{p}_percentile" for p in PERCENTILES_TO_CALC]
        + ["standard_deviation", "median"],
    )
    # TODO: these columns would be not even passed into and back out of measure_summary_float which
    # ignores them
    statistical_aggregate_final.drop(columns=["age_start", "age_end"], inplace=True)

    return statistical_aggregate_final


def africa_composite(
    wd: str | os.PathLike | Path,
    iu_metadata: IUData,
) -> Tuple[List[pd.DataFrame], pd.DataFrame]:
    canonical_ius = filter_to_maximum_year_range_for_all_ius(
        [
            pd.read_csv(iu.file_path)
            for iu in _tqdm_unknown_length(
                post_process_file_generator(
                    file_directory=output_directory_structure.get_canonical_dir(wd),
                    end_of_file="_canonical.csv",
                ),
                desc="Building Africa composite run",
            )
        ],
        keep_na_year_id=False
    )

    composite = composite_run.build_composite_run_multiple_scenarios(
        canonical_iu_runs=canonical_ius,
        iu_data=iu_metadata,
        is_africa=True,
    )

    # TODO(CA): Do we still need to write this composite file? Ask TK.
    # Currently the composite thing sticks a column for country based on the first IU which
    # isn't required for Africa, but this isn't a nice place to do this!
    # composite.drop(columns=[canonical_columns.COUNTRY_CODE], inplace=True)
    output_directory_structure.write_africa_composite(wd, composite)

    return canonical_ius, pd.read_csv(Path(wd) / "composite/africa_composite.csv")


def africa_lvl_aggregate(
    canonical_ius: List[pd.DataFrame],
    composite_africa: pd.DataFrame,
    prevalence_threshold: float = 0.01,
    pct_runs_threshold: List[float] = [0.9],
) -> pd.DataFrame:
    """
    Aggregates continent level prevalence and probability of extinction data.

    Args:
        canonical_ius (List[pd.DataFrame]): A list of dataframes containing canonical IU-level data.
        composite_africa (pd.DataFrame): A dataframe representing the composite prevalence data.
        prevalence_threshold (float): The prevalence threshold used to compute the probability
         of extinction. Defaults to 0.01.
        pct_runs_threshold (float): The fraction of draws that must be below the threshold when
        computing the `prop_ius_with_pct_runs_under_threshold`. Defaults to 0.9.

    Returns:
        pd.DataFrame: A dataframe with aggregated prevalence metrics and extinction probabilities.
    """
    extinction_dfs = _calc_extinction_metrics(
        canonical_ius, prevalence_threshold, pct_runs_threshold
    )

    # Collapse the prevalence from all the draws into an average metric
    africa_statistical_aggregate = aggregate_draws(composite_africa)
    general_columns = composite_africa[
        [
            canonical_columns.SCENARIO,
        ]
    ]

    africa_aggregates_complete = pd.concat([general_columns, africa_statistical_aggregate], axis=1)

    columns_to_use = [
        canonical_columns.SCENARIO,
        canonical_columns.MEASURE,
        canonical_columns.YEAR_ID,
        "mean",
    ]

    africa_prevalence_df = africa_aggregates_complete[
        columns_to_use
        + [f"{p}_percentile" for p in PERCENTILES_TO_CALC]
        + ["standard_deviation", "median"]
    ]

    for scenario in extinction_dfs:
        africa_prevalence_df = pd.concat(
            [
                africa_prevalence_df,
                extinction_dfs[scenario][columns_to_use],
            ],
            axis=0,
            ignore_index=True,
        )

    return africa_prevalence_df


def single_country_aggregate(composite_country_run: pd.DataFrame) -> pd.DataFrame:
    country_statistical_aggregates = aggregate_draws(composite_country_run)

    general_columns = composite_country_run[
        [
            canonical_columns.SCENARIO,
            canonical_columns.COUNTRY_CODE,
        ]
    ]

    country_aggregates_complete = pd.concat(
        [general_columns, country_statistical_aggregates], axis=1
    )
    return country_aggregates_complete[
        [
            canonical_columns.SCENARIO,
            canonical_columns.COUNTRY_CODE,
            canonical_columns.MEASURE,
            "year_id",
            "mean",
        ]
        + [f"{p}_percentile" for p in PERCENTILES_TO_CALC]
        + ["standard_deviation", "median"]
    ]


def country_lvl_aggregate(
    processed_iu_lvl_data: pd.DataFrame,
    threshold_summary_measure_names: list[str],
    threshold_groupby_cols: list[str],
    threshold_cols_rename: dict,
    pct_runs_under_threshold: list[float],
    denominator_to_use: int,
) -> pd.DataFrame:
    """
    Takes in an input dataframe of all the aggregated iu-lvl data and returns a summarized version
    with country level metrics.
    See constants.py for possible inputs to this function.
    Args:
        processed_iu_lvl_data (pd.Dataframe): the dataframe with all the iu-lvl data.
        threshold_summary_measure_names (list[str]): a list of measures names that we want to
                                            calculate the threshold statistics for
                                            (number of IU's that reach a threshold, calculated
                                            by the pct of non-null values).
                                            Example:["year_of_1_mfp_avg", "year_of_90_under_1_mfp"].
        threshold_groupby_cols (list[str]): a list of measures that we want to use to group the
                                            threshold statistics by.
                                            Example: ["scenario", "country_code", "measure"].
        threshold_cols_rename (dict): a dictionary of {"measure_name": "new_measure_name"} that will
                                    rename measures used to calculate the threshold
                                    statistics. Example:
                                    {"year_of_90_under_1_mfp":"pct_of_ius_passing_90pct_threshold",
                                    "year_of_1_mfp_avg":"pct_of_ius_passing_avg_threshold"}
        denominator_to_use (int): The value of the denominator for the given input

    Returns:
        A dataframe with country-level metrics
    """
    if len(threshold_summary_measure_names) == 0:
        if len(threshold_groupby_cols) > 0:
            raise ValueError(
                "The length of threshold_summary_measure_names is "
                + f"{len(threshold_summary_measure_names)} "
                + f"while threshold_groupby_cols is of length {len(threshold_groupby_cols)}. "
                + "threshold_summary_measure_names should be provided if the length of "
                + "threshold_groupby_cols is greater than 0."
            )
        raise ValueError("Threshold summary measures are required to be input.")

    yearly_pct_of_runs_dfs = []
    for pct in pct_runs_under_threshold:
        yearly_pct_of_runs_dfs.append(
            _yearly_pct_of_runs_threshold_summary_helper(
                processed_iu_lvl_data,
                list(set(threshold_groupby_cols) | {canonical_columns.YEAR_ID}),
                denominator_to_use,
                pct,
            )
        )

    summarize_threshold_year = _threshold_summary_helper(
        processed_iu_lvl_data,
        threshold_summary_measure_names,
        threshold_groupby_cols,
        year_all_ius_reach_threshold,
        "year_of_",
        threshold_cols_rename,
    )

    # Summary stuff
    summarize_threshold_year["mean"] = summarize_threshold_year["mean"].fillna(-1)

    return pd.concat(
        yearly_pct_of_runs_dfs + [summarize_threshold_year],
        axis=0,
        ignore_index=True,
    )

def filter_to_maximum_year_range_for_all_ius(
    all_iu_data: list[pd.DataFrame],
    keep_na_year_id: bool
) -> list[pd.DataFrame]:
    # Selecting the minimum starting year_id that exists for all IUs
    minimum_year = max(
        df["year_id"].min() for df in all_iu_data
    )
    # Selecting the maximum ending year_id that exists for all IUs
    maximum_year = min(
        df["year_id"].max() for df in all_iu_data
    )

    return [
        iu[
            (
                (iu["year_id"] >= minimum_year) &
                (iu["year_id"] <= maximum_year)
            ) |
            (
                # Need to keep columns with year_id == NA as certain calculated
                # iu metrics that are used to calculate percentage
                # stats at a country level do not have an associated year_id
                keep_na_year_id and iu["year_id"].isna()
            )
        # As we are returning a list of dataframes, the index is reset to make sure
        # that all the dataframes will be aligned properly.
        ].reset_index(drop=True) for iu in all_iu_data
    ]
