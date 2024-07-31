import glob
import csv
import pandas as pd
import numpy as np

AGGEGATE_DEFAULT_TYPING_MAP = {"year_id": float, "age_start": float, "age_end": float,
                               "mean": float, "2.5_percentile": float, "5_percentile": float,
                               "10_percentile": float, "25_percentile": float, "50_percentile": float,
                               "75_percentile": float, "90_percentile": float, "95_percentile": float,
                               "97.5_percentile": float, "median": float, "standard_deviation": float
                               }

def percentile(n):
        def percentile_(x):
            return np.percentile(x, n)
        percentile_.__name__ = '%s_percentile' % n
        return percentile_
def calc_not_na(x):
    return x.notnull().mean() * 100

def aggregatePostProcessedFiles(
    pathToOutputFiles: str = ".",
    specific_files: str = "*.csv",
    columns_to_replace_with_nan: list[str] = ["mean"],
    typingMap: dict = AGGEGATE_DEFAULT_TYPING_MAP
) -> pd.DataFrame:
    """
    Combines all data outputs in a given folder and filters as necessary. The format of the csv's in the folder should be the same output format of a call to `processSingleFile`

    Args:
        pathToOutputFiles (str): the top level folder where the output files are located.
        specific_files (str): file name filter to only combine the files that are wanted.
        columns_to_replace_with_nan (list[str]): any columns that should be replaced with NaN rather than None.
        typingMap (dict): a dictionary that maps column names to their dtype.
    
    Returns:
        A dataframe with all of the CSVs stacked ontop of one another
    """
    rows = []
    columns = []

    files_to_combine = glob.glob(pathToOutputFiles + "**/" + specific_files, recursive=True)
    index = 0
    total_files = len(files_to_combine)
    for filename in files_to_combine:
        index += 1
        with open(filename, newline="") as f:
            reader = csv.reader(f)
            if len(columns) == 0:
                columns = next(reader)
            else:
                next(reader)
            rows.extend(reader)
        if ((total_files >= 10) and (index % (total_files // 10) == 0)):
            print(f"Files Processed: {index / total_files * 100}%")
    aggregatedData = pd.DataFrame(rows, columns=columns)
    aggregatedData.loc[:, columns_to_replace_with_nan] = aggregatedData.loc[:, columns_to_replace_with_nan].replace("", np.nan)
    aggregatedData.loc[:, ~aggregatedData.columns.isin(columns_to_replace_with_nan)] = aggregatedData.loc[:, ~aggregatedData.columns.isin(columns_to_replace_with_nan)].replace("", None)
    aggregatedData = aggregatedData.astype(typingMap)
    return aggregatedData

def iuLevelAggregate(
    df: pd.DataFrame,
    filter_measures: list[str] = [],
    measure_column_name: str = "measure"
) -> pd.DataFrame:
    """
    Takes in an input dataframe of all the aggregated iu-lvl data and returns a filtered version if requested.

    Args:
        df (pd.Dataframe): the dataframe with all the iu-lvl data.
        filter_measures (list[str]): a list contain the measures you want to filter. if empty (default), then it will not filter anything.
        measure_column_name (list[str]): the name of the column where the measure names are located.
    
    Returns:
        A dataframe with all of the iu-lvl data, filtered or not
    """
    if len(filter_measures) > 0:
        df.loc[df[measure_column_name].isin(filter_measures), :]
    return df

def countryLevelAggregate(
    df: pd.DataFrame,
    measure_column_name: str = "measure",
    general_summary_cols: list[str] = ["prevalence", "year_of_1_mfp_avg", "year_of_90_under_1_mfp"],
    general_groupby_cols: list[str] = ["scenario", "country_code", "year_id", "age_start", "age_end", "measure"],
    threshold_summary_cols: list[str] = ["year_of_1_mfp_avg", "year_of_90_under_1_mfp"],
    threshold_groupby_cols: list[str] = ["scenario", "country_code", "measure"],
    threshold_cols_rename: dict = {"year_of_90_under_1_mfp":"pct_of_ius_passing_90pct_threshold", "year_of_1_mfp_avg":"pct_of_ius_passing_avg_threshold"}
) -> pd.DataFrame:
    """
    Takes in an input dataframe of all the aggregated iu-lvl data and returns a summarized version with country level metrics.

    Args:
        df (pd.Dataframe): the dataframe with all the iu-lvl data.
        measure_column_name (str, optional): the column where the measure names are located. 
                                                Defaults to 'measure'.
        general_summary_cols (list[str], optional): a list of measures that we want to generally summarize with mean, percentiles, std, and median. 
                                                    Defaults to ["prevalence", "year_of_1_mfp_avg", "year_of_90_under_1_mfp"].
        general_groupby_cols (list[str], optional): a list of columns that we want to use to group the general summaries by.
                                                    Defaults to ["scenario", "country_code", "year_id", "age_start", "age_end", "measure"].
        threshold_summary_cols (list[str], optional): a list of measures that we want to calculate the threshold statistics for (number of IU's that reach a threshold, calculated by the pct of non-null values).
                                                    Defaults to ["year_of_1_mfp_avg", "year_of_90_under_1_mfp"].
        threshold_groupby_cols (list[str], optional): a list of measures that we want to use to group the threshold statistics by.
                                                    Defaults to ["scenario", "country_code", "measure"].
        threshold_cols_rename (dict, optional): a dictionary of {"measure_name": "new_measure_name"} that will rename measures used to calculate the threshold statistics.
                                                    Defaults to {"year_of_90_under_1_mfp":"pct_of_ius_passing_90pct_threshold", "year_of_1_mfp_avg":"pct_of_ius_passing_avg_threshold"}
    
    Returns:
        A dataframe with country-level metrics
    """
    general_summary_df = df.loc[df[measure_column_name].isin(general_summary_cols), :].copy()
    # group by doesn't work with NaN/null values
    general_summary_df[general_groupby_cols] =  general_summary_df[general_groupby_cols].fillna(-1)
    general_summary = general_summary_df.groupby(general_groupby_cols).agg({
        "mean": ["mean",
                 percentile(2.5), percentile(5), percentile(10), percentile(25), percentile(50), 
                 percentile(75), percentile(90), percentile(95), percentile(97.5),
                 np.std, "median"
                ]
    }).reset_index()
    general_summary.columns = general_groupby_cols + ["mean", "2.5_percentile", "5_percentile", "10_percentile", "25_percentile", "50_percentile", "75_percentile", "90_percentile", "95_percentile", "97.5_percentile", "standard_deviation", "median"]
    general_summary[general_groupby_cols] = general_summary[general_groupby_cols].replace(-1, np.nan)

    summarize_threshold = df[df["measure"].isin(threshold_summary_cols)].groupby(threshold_groupby_cols).agg({
        "mean": [("mean", calc_not_na)]
    }).reset_index()
    summarize_threshold.columns = threshold_groupby_cols + ["mean"]
    summarize_threshold["measure"] = [
        threshold_cols_rename[measure_val]
        for measure_val in summarize_threshold["measure"]
        if measure_val in threshold_cols_rename
    ]
    return pd.concat([general_summary, summarize_threshold], axis=0, ignore_index=True)


def africaLevelAggregate(
    df: pd.DataFrame,
    measure_column_name: str = "measure",
    measures_to_summarize: list[str] = ["prevalence"],
    columns_to_group_by: list[str] = ["scenario", "year_id", "age_start", "age_end", "measure"]
) -> pd.DataFrame:
    """
    Takes in an input dataframe of all the aggregated country-level metrics and returns a summarized version with africa level metrics.

    Args:
        df (pd.Dataframe): the dataframe with country-level data.
        measure_column_name (str, optional): the column where the measure names are located. 
                                                Defaults to 'measure'.
        measures_to_summarize (list[str], optional): a list of measures that we want to generally summarize with mean, percentiles, std, and median. 
                                                    Defaults to ["prevalence"].
        columns_to_group_by (list[str], optional): a list of columns that we want to use to group the summaries by.
                                                    Defaults to ["scenario", "year_id", "age_start", "age_end", "measure"].
    
    Returns:
        A dataframe with africa-level metrics
    """
    africa_summary = df[df[measure_column_name].isin(measures_to_summarize)].groupby(columns_to_group_by).agg({
        "mean": ["mean",
                 percentile(2.5), percentile(5), percentile(10), percentile(25), percentile(50), 
                 percentile(75), percentile(90), percentile(95), percentile(97.5),
                 np.std, "median"
                ]
    }).reset_index()

    africa_summary.columns = columns_to_group_by + ["mean", "2.5_percentile", "5_percentile", "10_percentile", "25_percentile", "50_percentile", "75_percentile", "90_percentile", "95_percentile", "97.5_percentile", "standard_deviation", "median"]
    return(africa_summary)