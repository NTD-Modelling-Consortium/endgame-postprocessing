import glob
import csv
import pandas as pd
import numpy as np
from tqdm import tqdm
from post_processing_code.constants import PERCENTILES_TO_CALC

AGGEGATE_DEFAULT_TYPING_MAP = {
    "year_id": float,
    "age_start": float,
    "age_end": float,
    "mean": float,
    "2.5_percentile": float,
    "5_percentile": float,
    "10_percentile": float,
    "25_percentile": float,
    "50_percentile": float,
    "75_percentile": float,
    "90_percentile": float,
    "95_percentile": float,
    "97.5_percentile": float,
    "median": float,
    "standard_deviation": float,
}


def percentile(n):
    def percentile_(x):
        return np.percentile(x, n)

    percentile_.__name__ = "%s_percentile" % n
    return percentile_


def calc_not_na(x):
    return x.notnull().mean() * 100


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

    files_to_combine = glob.glob(path_to_files + "**/" + specific_files, recursive=True)
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
    path_to_files: str = ".",
    specific_files: str = "*.csv",
    filter_measures: list[str] = [],
    measure_column_name: str = "measure",
    columns_to_replace_with_nan: list[str] = ["mean"],
    typing_map: dict = AGGEGATE_DEFAULT_TYPING_MAP,
) -> pd.DataFrame:
    """
    A wrapper function for aggregate_post_processed_files that takes in a path to where the data is
    located, and returns aggregated iu-lvl data, with columns modified to the correct types, and
    filtered if requested.

    Args:
        path_to_files (str): the top level folder where the output files are located.
        specific_files (str): file name filter to only combine the files that are wanted.
        columns_to_replace_with_nan (list[str]): any columns that should be replaced with NaN rather
                                                    than None.
        typing_map (dict): a dictionary that maps column names to their dtype.
        df (pd.Dataframe): the dataframe with all the iu-lvl data.
        filter_measures (list[str]): a list contain the measures you want to filter.
                                        If empty (default), then it will not filter anything.
        measure_column_name (list[str]): the name of the column where the measure names are located.

    Returns:
        A dataframe with all of the iu-lvl data, filtered or not
    """
    aggregated_data = aggregate_post_processed_files(path_to_files, specific_files)
    # filter to use only the measures requested
    if len(filter_measures) > 0:
        aggregated_data.loc[
            aggregated_data[measure_column_name].isin(filter_measures), :
        ]

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


def country_lvl_aggregate(
    iu_lvl_data: pd.DataFrame,
    measure_column_name: str,
    general_summary_cols: list[str],
    general_groupby_cols: list[str],
    threshold_summary_cols: list[str],
    threshold_groupby_cols: list[str],
    threshold_cols_rename: dict,
) -> pd.DataFrame:
    """
    Takes in an input dataframe of all the aggregated iu-lvl data and returns a summarized version
    with country level metrics.
    See constants.py for possible inputs to this function.
    Args:
        iu_lvl_data (pd.Dataframe): the dataframe with all the iu-lvl data.
        measure_column_name (str): the column where the measure names are located.
                                            Example: 'measure'.
        general_summary_cols (list[str]): a list of measures that we want to generally summarize
                                            with mean, percentiles, std, and median.
                                            Example: ["prevalence", "year_of_1_mfp_avg",
                                            "year_of_90_under_1_mfp"].
        general_groupby_cols (list[str]): a list of columns that we want to use to group the general
                                            summaries by.
                                            Example: ["scenario", "country_code", "year_id",
                                            "age_start", "age_end", "measure"].
        threshold_summary_cols (list[str]): a list of measures that we want to calculate the
                                            threshold statistics for (number of IU's that reach a
                                            threshold, calculated by the pct of non-null values).
                                            Example:["year_of_1_mfp_avg", "year_of_90_under_1_mfp"].
        threshold_groupby_cols (list[str]): a list of measures that we want to use to group the
                                            threshold statistics by.
                                            Example: ["scenario", "country_code", "measure"].
        threshold_cols_rename (dict): a dictionary of {"measure_name": "new_measure_name"} that will
                                    rename measures used to calculate the threshold
                                    statistics. Example:
                                    {"year_of_90_under_1_mfp":"pct_of_ius_passing_90pct_threshold",
                                    "year_of_1_mfp_avg":"pct_of_ius_passing_avg_threshold"}

    Returns:
        A dataframe with country-level metrics
    """
    general_summary_df = iu_lvl_data.loc[
        iu_lvl_data[measure_column_name].isin(general_summary_cols), :
    ].copy()
    # group by doesn't work with NaN/null values
    general_summary_df[general_groupby_cols] = general_summary_df[
        general_groupby_cols
    ].fillna(-1)
    general_summary = (
        general_summary_df.groupby(general_groupby_cols)
        .agg(
            {
                "mean": ["mean"]
                + [percentile(p) for p in PERCENTILES_TO_CALC]
                + [np.std, "median"]
            }
        )
        .reset_index()
    )
    general_summary.columns = (
        general_groupby_cols
        + ["mean"]
        + [str(p) + "_percentile" for p in PERCENTILES_TO_CALC]
        + ["standard_deviation", "median"]
    )
    general_summary[general_groupby_cols] = general_summary[
        general_groupby_cols
    ].replace(-1, np.nan)

    summarize_threshold = (
        iu_lvl_data[iu_lvl_data["measure"].isin(threshold_summary_cols)]
        .groupby(threshold_groupby_cols)
        .agg({"mean": [("mean", calc_not_na)]})
        .reset_index()
    )
    summarize_threshold.columns = threshold_groupby_cols + ["mean"]
    summarize_threshold["measure"] = [
        threshold_cols_rename[measure_val]
        for measure_val in summarize_threshold["measure"]
        if measure_val in threshold_cols_rename
    ]
    return pd.concat([general_summary, summarize_threshold], axis=0, ignore_index=True)


def africa_lvl_aggregate(
    country_lvl_data: pd.DataFrame,
    measure_column_name: str,
    measures_to_summarize: list[str],
    columns_to_group_by: list[str],
) -> pd.DataFrame:
    """
    Takes in an input dataframe of all the aggregated country-level metrics and returns a summarized
    version with africa level metrics.

    Args:
        country_lvl_data (pd.Dataframe): the dataframe with country-level data.
        measure_column_name (str): the column where the measure names are located.
                                                Examle: 'measure'.
        measures_to_summarize (list[str]): a list of measures that we want to generally summarize
                                            with mean, percentiles, std, and median.
                                            Examle: ["prevalence"].
        columns_to_group_by (list[str]): a list of columns that we want to use
                                            to group the summaries by.
                                            Examle: ["scenario", "year_id", "age_start",
                                            "age_end", "measure"].

    Returns:
        A dataframe with africa-level metrics
    """
    africa_summary = (
        country_lvl_data[
            country_lvl_data[measure_column_name].isin(measures_to_summarize)
        ]
        .groupby(columns_to_group_by)
        .agg(
            {
                "mean": ["mean"]
                + [percentile(p) for p in PERCENTILES_TO_CALC]
                + [np.std, "median"]
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
