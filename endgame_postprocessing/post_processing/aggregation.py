import glob
import csv
import os
import pandas as pd
from endgame_postprocessing.model_wrappers.constants import COUNTRY_SUMMARY_GROUP_COLUMNS
from endgame_postprocessing.post_processing import canoncical_columns
from endgame_postprocessing.post_processing.measures import measure_summary_float
import numpy as np
from tqdm import tqdm
from .constants import (
    AGE_END_COLUMN_NAME,
    AGE_START_COLUMN_NAME,
    DEFAULT_PREVALENCE_MEASURE_NAME,
    DRAW_COLUMNN_NAME_START,
    MEASURE_COLUMN_NAME,
    NUM_DRAWS,
    PERCENTILES_TO_CALC,
    AGGEGATE_DEFAULT_TYPING_MAP,
    YEAR_COLUMN_NAME,
)


def _percentile(n):
    def percentile_(x):
        return np.percentile(x, n)

    percentile_.__name__ = "%s_percentile" % n
    return percentile_


def _calc_prob_not_na(x):
    return x.notnull().mean() * 100


def _calc_sum_not_na(x):
    return x.notnull().sum()

def _calc_max_not_na(x):
    return x.max()

def add_scenario_and_country_to_raw_data(data, scenario_name, iu_name):
    data["scenario_name"] = scenario_name
    data["country_code"] = iu_name[:3]
    data["iu_name"] = iu_name
    return(data)

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
    files_to_combine = glob.glob(
        properly_terminated_path + "**/" + specific_files, recursive=True
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


def single_country_aggregate(composite_country_run: pd.DataFrame) -> pd.DataFrame:
    draw_names = list(
        [
            draw_column
            for draw_column in composite_country_run.columns
            if draw_column.startswith(DRAW_COLUMNN_NAME_START)
        ]
    )
    country_statistical_aggregate = measure_summary_float(
        data_to_summarize=composite_country_run.to_numpy(),
        year_id_loc=composite_country_run.columns.get_loc(canoncical_columns.YEAR_ID),
        measure_column_loc=composite_country_run.columns.get_loc(
            canoncical_columns.MEASURE
        ),
        age_start_loc=0,
        age_end_loc=0,
        draws_loc=[composite_country_run.columns.get_loc(name) for name in draw_names],
    )
    country_statistical_aggregates = pd.DataFrame(
        country_statistical_aggregate,
        columns=["year_id", "age_start", "age_end", "measure", "mean"]
        + [f"{p}_percentile" for p in PERCENTILES_TO_CALC]
        + ["std", "median"],
    )

    # TODO: these columns would be not even passed into and back out of measure_summary_float which
    # ignores them
    country_statistical_aggregates.drop(columns=["age_start", "age_end"], inplace=True)

    general_columns = composite_country_run[
        [
            canoncical_columns.SCENARIO,
            canoncical_columns.COUNTRY_CODE,
        ]
    ]

    country_aggregates_complete = pd.concat(
        [general_columns, country_statistical_aggregates], axis=1
    )
    country_aggregates_complete = country_aggregates_complete[
        [
            canoncical_columns.SCENARIO,
            canoncical_columns.COUNTRY_CODE,
            canoncical_columns.MEASURE,
            "year_id",
            "mean",
        ]
        + [f"{p}_percentile" for p in PERCENTILES_TO_CALC]
        + ["std", "median"]
    ]
    return country_aggregates_complete


def country_lvl_aggregate(
    raw_iu_data: pd.DataFrame,
    processed_iu_lvl_data: pd.DataFrame,
    general_summary_measure_names: list[str],
    general_groupby_cols: list[str],
    threshold_summary_measure_names: list[str],
    threshold_groupby_cols: list[str],
    threshold_cols_rename: dict,
    path_to_population_data: str,
) -> pd.DataFrame:
    """
    Takes in an input dataframe of all the aggregated iu-lvl data and returns a summarized version
    with country level metrics.
    See constants.py for possible inputs to this function.
    Args:
        iu_lvl_data (pd.Dataframe): the dataframe with all the iu-lvl data.
        general_summary_measure_names (list[str]): a list of measures that we want to generally
                                            summarize with mean, percentiles, std, and median.
                                            Example: ["prevalence", "year_of_1_mfp_avg",
                                            "year_of_90_under_1_mfp"].
        general_groupby_cols (list[str]): a list of columns that we want to use to group the general
                                            summaries by.
                                            Example: ["scenario", "country_code", "year_id",
                                            "age_start", "age_end", "measure"].
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
        path_to_population_data (str): path to a population dataset that will contain the population
                                    for each IU within the `iu_lvl_data`.

    Returns:
        A dataframe with country-level metrics
    """
    draw_names = [f"{DRAW_COLUMNN_NAME_START}{i}" for i in range(0, NUM_DRAWS)]

    ### Uncomment below once actual population file is provided, most of this should be in the composite code
    # population_data = pd.read_csv(
    #     path_to_population_data,
    #     usecols=["country_code", "iu_name", "population", "is_endemic", "is_modelled"]
    # )

    # grouped_population_data = _group_country_pop(population_data, "is_endemic")

    # new_population_data = population_data.merge(
    #     grouped_population_data,
    #     how="inner",
    #     on=["country_code"]
    # )

    # weighted_prevalence_data = raw_iu_data.merge(
    #     new_population_data,
    #     how="left",
    #     left_on=["iu_name", "country_code"],
    #     right_on=["iu_name", "country_code"]
    # )

    # to do: this should use DEFAULT_PREVALENCE_MEASURE_NAME for the composite file
    weighted_prevalence_data = raw_iu_data.loc[
        raw_iu_data[MEASURE_COLUMN_NAME] == "prevalence",
        :
    ].copy()
    weighted_prevalence_data["population"] = 10000
    weighted_prevalence_data["country_pop"] = 1000000

    # weighting IU mean by population
    population_array = weighted_prevalence_data["population"].values
    country_pop_array = weighted_prevalence_data["country_pop"].values
    draw_data = weighted_prevalence_data[draw_names].values

    population_weight = population_array / country_pop_array
    weighted_prevalence_data[draw_names] = draw_data * population_weight[:, np.newaxis]

    weighted_prevalence_data_by_country = (
        weighted_prevalence_data
            .groupby(general_groupby_cols)[draw_names]
            .sum()
            .reset_index()
    )

    weighted_column_names = weighted_prevalence_data_by_country.columns
    weighted_summarised_prevalence_data = pd.DataFrame(
        np.column_stack((
            weighted_prevalence_data_by_country[["scenario_name", "country_code"]].to_numpy(),
            measure_summary_float(
                data_to_summarize=weighted_prevalence_data_by_country.to_numpy(),
                year_id_loc=weighted_column_names.get_loc(YEAR_COLUMN_NAME),
                measure_column_loc=weighted_column_names.get_loc(MEASURE_COLUMN_NAME),
                age_start_loc=weighted_column_names.get_loc(AGE_START_COLUMN_NAME),
                age_end_loc=weighted_column_names.get_loc(AGE_END_COLUMN_NAME),
                draws_loc=[weighted_column_names.get_loc(name) for name in draw_names]
            )
        )),
        columns=(
            general_groupby_cols +
            ["mean"] +
            [f"{p}_percentile" for p in PERCENTILES_TO_CALC] +
            ["std", "median"]
        )
    )

    if (len(threshold_summary_measure_names) == 0):
        if len(threshold_groupby_cols) > 0:
            raise ValueError(
                "The length of threshold_summary_measure_names is " +
                f"{len(threshold_summary_measure_names)} " +
                f"while threshold_groupby_cols is of length {len(threshold_groupby_cols)}. " +
                "threshold_summary_measure_names should be provided if the length of " +
                "threshold_groupby_cols is greater than 0."
            )
        return weighted_summarised_prevalence_data

    summarize_threshold = _threshold_summary_helper(
        processed_iu_lvl_data,
        threshold_summary_measure_names,
        threshold_groupby_cols,
        _calc_prob_not_na,
        "pct_of_",
        threshold_cols_rename
    )

    summarize_threshold_counts = _threshold_summary_helper(
        processed_iu_lvl_data,
        threshold_summary_measure_names,
        threshold_groupby_cols,
        _calc_sum_not_na,
        "count_of_",
        threshold_cols_rename
    )

    summarize_threshold_year = _threshold_summary_helper(
        processed_iu_lvl_data,
        threshold_summary_measure_names,
        threshold_groupby_cols,
        _calc_max_not_na,
        "year_of_",
        threshold_cols_rename
    )

    # Summary stuff
    summarize_threshold_year["mean"] = summarize_threshold_year["mean"] - 1
    summarize_threshold_year["mean"] = summarize_threshold_year["mean"].fillna(-1)

    return pd.concat(
        [weighted_summarised_prevalence_data, summarize_threshold, summarize_threshold_counts, summarize_threshold_year],
        axis=0,
        ignore_index=True,
    )


def africa_lvl_aggregate(
    country_lvl_data: pd.DataFrame,
    measures_to_summarize: list[str],
    columns_to_group_by: list[str],
) -> pd.DataFrame:
    """
    Takes in an input dataframe of all the aggregated country-level metrics and returns a summarized
    version with africa level metrics.

    Args:
        country_lvl_data (pd.Dataframe): the dataframe with country-level data.
        measures_to_summarize (list[str]): a list of measures that we want to generally summarize
                                            with mean, percentiles, std, and median.
                                            Example: ["prevalence"].
        columns_to_group_by (list[str]): a list of columns that we want to use
                                            to group the summaries by.
                                            Example: ["scenario", "year_id", "age_start",
                                            "age_end", "measure"].

    Returns:
        A dataframe with africa-level metrics
    """
    africa_summary = (
        country_lvl_data[
            country_lvl_data[MEASURE_COLUMN_NAME].isin(measures_to_summarize)
        ]
        .groupby(columns_to_group_by)
        .agg(
            {
                "mean": ["mean"]
                + [_percentile(p) for p in PERCENTILES_TO_CALC]
                + ["std", "median"]
            }
        )
        .reset_index()
    )

    africa_summary.columns = (
        columns_to_group_by
        + ["mean"]
        + [str(p) + "_percentile" for p in PERCENTILES_TO_CALC]
        + ["standard_deviation", "median"]
    )
    return africa_summary
