import csv
import glob
import os
from functools import partial
from pathlib import Path

import numpy as np
import pandas as pd
from tqdm import tqdm

from endgame_postprocessing.post_processing import output_directory_structure, composite_run, canonical_columns
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


def add_scenario_and_country_to_raw_data(data, scenario_name, iu_name):
    data["scenario_name"] = scenario_name
    data["country_code"] = iu_name[:3]
    data["iu_name"] = iu_name
    return (data)


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
    aggregated_data.loc[
    :, ~aggregated_data.columns.isin(columns_to_replace_with_nan)
    ] = aggregated_data.loc[
        :, ~aggregated_data.columns.isin(columns_to_replace_with_nan)
        ].replace(
        "", None
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
        measure_rename_map
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
        processed_iu_lvl_data,
        group_by_cols,
        denominator_to_use,
        pct_of_runs
):
    summarize_threshold = _threshold_summary_helper(
        processed_iu_lvl_data,
        [PROB_UNDER_THRESHOLD_MEASURE_NAME],
        group_by_cols,
        partial(
            _calc_count_of_pct_runs,
            pct_of_runs=pct_of_runs,
            denominator_val=denominator_to_use
        ),
        "pct_of_",
        {
            PROB_UNDER_THRESHOLD_MEASURE_NAME:
                f"ius_with_{int(pct_of_runs * 100)}pct_runs_under_threshold"
        },
    )

    summarize_threshold_counts = _threshold_summary_helper(
        processed_iu_lvl_data,
        [PROB_UNDER_THRESHOLD_MEASURE_NAME],
        group_by_cols,
        partial(_calc_count_of_pct_runs, pct_of_runs=pct_of_runs, denominator_val=1),
        "count_of_",
        {
            PROB_UNDER_THRESHOLD_MEASURE_NAME:
                f"ius_with_{int(pct_of_runs * 100)}pct_runs_under_threshold"
        },
    )
    return pd.concat([summarize_threshold, summarize_threshold_counts], axis=0, ignore_index=True)


def aggregate_draws(composite_data: pd.DataFrame) -> pd.DataFrame:
    draw_names = list(filter(lambda name: name.startswith(DRAW_COLUMNN_NAME_START), composite_data.columns))
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
    # TODO(CA, 31.1.2025): Compute proportion of draws under threshold using `calc_prob_under_threshold` and append to dataframe

    # TODO: these columns would be not even passed into and back out of measure_summary_float which
    # ignores them
    statistical_aggregate_final.drop(columns=["age_start", "age_end"], inplace=True)

    return statistical_aggregate_final


def africa_lvl_aggregate(wd: str | os.PathLike | Path,
                         iu_metadata: IUData,
                         prevalence_threshold: float = 0.01) -> pd.DataFrame:
    canonical_ius = [pd.read_csv(iu.file_path) for iu in
                     tqdm(post_process_file_generator(
                         file_directory=output_directory_structure.get_canonical_dir(wd),
                         end_of_file="_canonical.csv",
                     ), desc='Building Africa composite run')]

    africa_composite = composite_run.build_composite_run_multiple_scenarios(canonical_iu_runs=canonical_ius,
                                                                            iu_data=iu_metadata,
                                                                            prevalence_threshold=prevalence_threshold,
                                                                            is_africa=True)

    # TODO(CA): Do we still need to write this composite file? Ask TK.
    # Currently the composite thing sticks a column for country based on the first IU which
    # isn't required for Africa, but this isn't a nice place to do this!
    # africa_composite.drop(columns=[canonical_columns.COUNTRY_CODE], inplace=True)
    output_directory_structure.write_africa_composite(
        wd, africa_composite
    )

    # Collapse the prevalence from all the draws into an average metric
    africa_statistical_aggregate = aggregate_draws(africa_composite)
    general_columns = africa_composite[
        [
            canonical_columns.SCENARIO,
        ]
    ]

    africa_aggregates_complete = pd.concat(
        [general_columns, africa_statistical_aggregate], axis=1
    )
    return africa_aggregates_complete[
        [
            canonical_columns.SCENARIO,
            canonical_columns.MEASURE,
            "year_id",
            "mean",
        ]
        + [f"{p}_percentile" for p in PERCENTILES_TO_CALC]
        + ["standard_deviation", "median"]
        ]


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
    if (len(threshold_summary_measure_names) == 0):
        if len(threshold_groupby_cols) > 0:
            raise ValueError(
                "The length of threshold_summary_measure_names is " +
                f"{len(threshold_summary_measure_names)} " +
                f"while threshold_groupby_cols is of length {len(threshold_groupby_cols)}. " +
                "threshold_summary_measure_names should be provided if the length of " +
                "threshold_groupby_cols is greater than 0."
            )
        raise ValueError(
            "Threshold summary measures are required to be input."
        )

    yearly_pct_of_runs_dfs = []
    for pct in pct_runs_under_threshold:
        yearly_pct_of_runs_dfs.append(_yearly_pct_of_runs_threshold_summary_helper(
            processed_iu_lvl_data,
            list(set(threshold_groupby_cols) | {canonical_columns.YEAR_ID}),
            denominator_to_use,
            pct
        ))

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
