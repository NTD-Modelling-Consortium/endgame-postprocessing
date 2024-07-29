import numpy as np
import pandas as pd
import glob
import csv
import os


def processSingleFile(
    data_path: str,
    scenario: str,
    iuName: str,
    output_loc: str,
    prevalence_marker_name: str = "prevalence",
    model: str = "oncho",
    post_processing_start_time: int = 1970,
) -> None:
    """
    Takes in non-age-grouped model outputs and generates a summarized output file that summarizes
    the model outputs into mean, median, standard deviation, 2.5, 5, 10, 25, 50, 75, 90, 95, and 97.5 percentiles.
    It also calculates:
        (for each year) the probability that < 1% mf prevalence was reached across all iterations/draws
        (for each year) the probability that < 0% mf prevalence was reached across all iterations/draws
        the year at which < 1% mf prevalence is reached, calculated using the average prevalence across all runs
        the year at which 90% of runs reach < 1% mf prevalence is reached

    Args:
        data list[Data]: The raw data output from multiple runs of the model (a list of dictionaries, where each dictionary is the outputs for a single run of the model)
        iuName str: A name to define the parameters used for the model, typically the name of the IU being simulated
        scenario str: A name to define the scenario being tested
        prevalence_marker_name str: The name for the prevalence marker to be used to calculate the additional outputs
        post_processing_start_time int: The time at which we start the calculations of reaching the threshold/elimination
        csv_file str: The name you want the post processed data to be saved to.
        mda_start_year int: An optional variable to denote when MDA starts for a scenario
        mda_stop_year int: An optional variable to denote when MDA ends for a given scenario
        mda_interval int: An optional variable to denote the frequency of the MDA applied for a given scenario
    """
    #scenario = data_path.split("/")[-1].split("-")[4]
    #iuName = data_path.split("/")[-1].split("-")[2]
    tmp = pd.read_csv(data_path)
    column_names = tmp.columns
    tmp = tmp.to_numpy()

    measure_column_loc = column_names.get_loc("measure")
    year_column_loc = column_names.get_loc("year_id")
    draws_start_loc = column_names.get_loc("draw_0")

    # Calculating probability of elimination using mf_prev
    mf_prev_mask = tmp[:, measure_column_loc] == prevalence_marker_name
    mf_prev_vals = tmp[mf_prev_mask, draws_start_loc:].astype(float)

    # Data manipulation
    # Making sure we start the calculations from where we want
    post_processing_start_mask = tmp[:, year_column_loc].astype(float) >= post_processing_start_time
    tmp_for_calc = tmp[post_processing_start_mask, :]

    # Calculating probability of elimination using mf_prev
    mf_prev_mask = tmp_for_calc[:, measure_column_loc] == prevalence_marker_name
    mf_prev_vals = tmp_for_calc[mf_prev_mask, draws_start_loc:].astype(float)

    # Calculating the year where each run has < 1% prev
    mf_under_1_mask = mf_prev_vals < 0.01

    # Probability of getting < 1% mfp for a given year
    prob_under_1_mfp = np.mean(mf_under_1_mask, axis=1)
    num_prob_under_1_mfp = len(prob_under_1_mfp)
    blank_array = np.full(num_prob_under_1_mfp, "")
    nan_array = np.full(num_prob_under_1_mfp, np.nan)
    none_array = np.full(num_prob_under_1_mfp, None)
    prob_under_1_mfp_output = np.column_stack(
        (
            blank_array,
            nan_array,
            nan_array,
            np.full(num_prob_under_1_mfp, "prob_under_1_mfp"),
            prob_under_1_mfp,
            none_array,
            none_array,
            none_array,
            none_array,
            none_array,
            none_array,
            none_array,
            none_array,
            none_array,
            none_array,
            none_array,
        )
    )

    indeces_of_90_under_1_mfp = np.where(prob_under_1_mfp >= 0.90)[0]
    year_of_90_under_1_mfp = None
    if indeces_of_90_under_1_mfp.size > 0:
        year_of_90_under_1_mfp = tmp_for_calc[mf_prev_mask, :][
            indeces_of_90_under_1_mfp[0], year_column_loc
        ]
    year_90_under_1_mfp_output = np.column_stack(
        (
            "",
            np.nan,
            np.nan,
            "year_of_90_under_1_mfp",
            year_of_90_under_1_mfp,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
    )

    # Calculating the year where the avg of all runs has < 1% mf prev
    yearly_avg_mfp = np.mean(mf_prev_vals, axis=1)
    indeces_of_1_mfp_avg = np.where(yearly_avg_mfp < 0.01)[0]
    year_of_1_mfp_avg = None
    if indeces_of_1_mfp_avg.size > 0:
        year_of_1_mfp_avg = tmp_for_calc[mf_prev_mask, :][indeces_of_1_mfp_avg[0], year_column_loc]
    year_under_1_avg_mfp_output = np.column_stack(
        (
            "",
            np.nan,
            np.nan,
            "year_of_1_mfp_avg",
            year_of_1_mfp_avg,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
    )

    # Summarizing all other outputs
    other_prevs = tmp[:, draws_start_loc:].astype(float)
    other_prevs_output = np.column_stack(
        (
            tmp[:, year_column_loc:draws_start_loc],
            np.mean(other_prevs, axis=1),
            np.percentile(other_prevs, 2.5, axis=1),
            np.percentile(other_prevs, 5, axis=1),
            np.percentile(other_prevs, 10, axis=1),
            np.percentile(other_prevs, 25, axis=1),
            np.percentile(other_prevs, 50, axis=1),
            np.percentile(other_prevs, 75, axis=1),
            np.percentile(other_prevs, 90, axis=1),
            np.percentile(other_prevs, 95, axis=1),
            np.percentile(other_prevs, 97.5, axis=1),
            np.std(other_prevs, axis=1),
            np.median(other_prevs, axis=1),
        )
    )
    output = np.row_stack(
        (
            other_prevs_output,
            # probability of 1% mf_prev for each year
            prob_under_1_mfp_output,
            # year that the avg mfp is < 1%
            year_under_1_avg_mfp_output,
            # year that the 90% of runs have < 1% mfp
            year_90_under_1_mfp_output,
        )
    )

    descriptor_output = np.column_stack(
        (
            np.full(output.shape[0], iuName),
            # extracting country code
            np.full(output.shape[0], iuName[:3]),
            np.full(output.shape[0], scenario),
            output,
        )
    )

    pd.DataFrame(
        descriptor_output,
        columns=[
            "iu_name",
            "country_code",
            "scenario",
            "year_id",
            "age_start",
            "age_end",
            "measure",
            "mean",
            "2.5_percentile",
            "5_percentile",
            "10_percentile",
            "25_percentile",
            "50_percentile",
            "75_percentile",
            "90_percentile",
            "95_percentile",
            "97.5_percentile",
            "standard_deviation",
            "median",
        ],
    ).to_csv((str(output_loc) + model + "_" + iuName + "_" + scenario + "_post-processed.csv"))