import glob
import csv
import pandas as pd
import numpy as np


def aggregateAndCalculate(
    pathToOutputFiles=".",
    specific_files="*.csv",
    output_file_root="",
):
    """
    Combines all data outputs in a given folder and filters as necessary. Saves it into two new files, one which is filtered, and one which is not.
    The data in the folder should be the output of `post_processing_calculation`

    Args:
        pathToOutputFiles - the top level folder where the output files are located
        specific_files - file name filter to only combine the files that are wanted
        output_file_root - path + root name of the output file i.e "path/to/data/test". files will be saved by appending "-iu_lvl.csv", "-country_lvl.csv", and "-africa.csv" to the root
    """

    rows = []
    columns = []

    for filename in glob.glob(
        pathToOutputFiles + "**/" + specific_files, recursive=True
    ):
        with open(filename, newline="") as f:
            reader = csv.reader(f)
            if len(columns) == 0:
                columns = next(reader)
            else:
                next(reader)
            rows.extend(reader)

    all_iu_data = pd.DataFrame(rows, columns=columns)
    all_iu_data[(all_iu_data["mean"] == "")] = np.nan
    all_iu_data.replace("", np.nan, inplace=True)
    all_iu_data = all_iu_data.astype({"year_id":float, "age_start":float, "age_end":float,
                                      "mean": float, "2.5_percentile": float, "5_percentile": float, 
                                      "10_percentile": float, "25_percentile": float, "50_percentile": float, 
                                      "75_percentile": float, "90_percentile": float, "95_percentile": float, 
                                      "97.5_percentile": float, "median": float, "standard_deviation": float
                                      })
    all_iu_data.to_csv(f"{output_file_root}-iu_lvl_data.csv")

    def percentile(n):
        def percentile_(x):
            return np.percentile(x, n)
        percentile_.__name__ = '%s_percentile' % n
        return percentile_
    def calc_not_na(x):
        return x.notnull().mean() * 100

    all_iu_data.loc[all_iu_data["measure"].isin(["year_of_1_mfp_avg", "year_of_90_under_1_mfp_avg"]), ["year_id", "age_start", "age_end"]] = -1
    summarize_prevalence = all_iu_data[all_iu_data["measure"].isin(["prevalence", "year_of_1_mfp_avg", "year_of_90_under_1_mfp_avg"])].groupby(["country_code", "year_id", "age_start", "age_end", "measure"]).agg({
        "mean": ["mean",
                 percentile(2.5), percentile(5), percentile(10), percentile(25), percentile(50), 
                 percentile(75), percentile(90), percentile(95), percentile(97.5),
                 "std", "median"
                ]
    }).reset_index()
    summarize_prevalence.columns = ["country_code", "year_id", "age_start", "age_end", "measure", "mean", "2.5_percentile", "5_percentile", "10_percentile", "25_percentile", "50_percentile", "75_percentile", "90_percentile", "95_percentile", "97.5_percentile", "standard_deviation", "median"]
    summarize_prevalence.loc[summarize_prevalence["measure"].isin(["year_of_1_mfp_avg", "year_of_90_under_1_mfp_avg"]), ["year_id", "age_start", "age_end"]] = np.NaN
    
    summarize_threshold = all_iu_data[all_iu_data["measure"].isin(["year_of_1_mfp_avg", "year_of_90_under_1_mfp_avg"])].groupby(["country_code", "measure"]).agg({
        "mean": [("mean", calc_not_na)]
    }).reset_index()
    summarize_threshold.columns = ["country_code", "measure", "mean"]
    summarize_threshold["measure"] = np.where(summarize_threshold["measure"] == "year_of_90_under_1_mfp_avg", "pct_of_ius_passing_90pct_threshold", "pct_of_ius_passing_avg_threshold")
    country_summary = pd.concat([summarize_prevalence, summarize_threshold], axis=0, ignore_index=True)
    country_summary.to_csv(f"{output_file_root}-country_lvl_data.csv")

    africa_summary = country_summary[country_summary["measure"] == "prevalence"].groupby(["year_id", "age_start", "age_end", "measure"]).agg({
        "mean": ["mean",
                 percentile(2.5), percentile(5), percentile(10), percentile(25), percentile(50), 
                 percentile(75), percentile(90), percentile(95), percentile(97.5),
                 "std", "median"
                ]
    }).reset_index()


    africa_summary.columns = ["year_id", "age_start", "age_end", "measure", "mean", "2.5_percentile", "5_percentile", "10_percentile", "25_percentile", "50_percentile", "75_percentile", "90_percentile", "95_percentile", "97.5_percentile", "standard_deviation", "median"]
    africa_summary.to_csv(f"{output_file_root}-africa_data.csv")