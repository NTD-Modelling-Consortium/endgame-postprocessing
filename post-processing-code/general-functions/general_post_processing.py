import numpy as np
import pandas as pd


# todo: have the post processing be unique functions to call, or use a class with attributes

def measure_summary_float(data, descriptor_column_locs, draws_loc):
    values = data[:, draws_loc].astype(float)
    return np.column_stack(
        (
            data[:, descriptor_column_locs],
            np.mean(values, axis=1),
            np.percentile(values, 2.5, axis=1),
            np.percentile(values, 5, axis=1),
            np.percentile(values, 10, axis=1),
            np.percentile(values, 25, axis=1),
            np.percentile(values, 50, axis=1),
            np.percentile(values, 75, axis=1),
            np.percentile(values, 90, axis=1),
            np.percentile(values, 95, axis=1),
            np.percentile(values, 97.5, axis=1),
            np.std(values, axis=1),
            np.median(values, axis=1),
        )
    )

def processSingleFile(
    df: pd.DataFrame,
    scenario: str,
    iuName: str,
    year_column_name = "year_id",
    measure_column_name = "measure",
    age_start_column_name = "age_start",
    age_end_column_name = "age_end",
    draw_names: str = [f'draw_{i}' for i in range(0, 200)],
    prevalence_marker_name: str = "prevalence",
    post_processing_start_time: int = 1970,
    threshold: float = 0.01,
    pct_runs_under_threshold: float = 0.90,
    measure_summary_map: dict = {}
) -> None:
    """
    Takes in non-age-grouped model outputs and generates a summarized output file that summarizes requested measure values into
    into mean, median, standard deviation, 2.5, 5, 10, 25, 50, 75, 90, 95, and 97.5 percentiles.
    It also calculates:
        (for each year) the probability that < 1% mf prevalence was reached across all iterations/draws
        (for each year) the probability that < 0% mf prevalence was reached across all iterations/draws
        the year at which < 1% mf prevalence is reached, calculated using the average prevalence across all runs
        the year at which 90% of runs reach < 1% mf prevalence is reached

    Args:
        df (pd.DataFrame): The raw data output in the form of a dataframe from multiple runs of the model.
        iuName (str): A name to define the parameters used for the model, typically the name of the IU being simulated.
        year_column_name = "year_id",
        measure_column_name = "measure",
        age_start_column_name = "age_start",
        age_end_column_name = "age_end",
        draw_names: str = [f'draw_{i}' for i in range(0, 200)],
        prevalence_marker_name (str): The name of the prevalence measure that is used to compare with the threhsold.
                                    Default is "prevalence".
        post_processing_start_time (int): The time at which we want to start calculating the different metrics.
                                            Default is 1970.          
        threshold (float): the value of the threshold we compare prevalence values to (value < threhsold).
                                            Default is 0.01.
        pct_runs_under_threshold (float): the percent of runs that should reach the given threshold (pct_runs >= pct_runs_under_threshold).
                                            Default is 0.90.
        measure_summary_map (dict[str: function], optional): a map of measure names to a summary calculation functions. This will be used to calculate the mean, median, std, and percentiles.
                                                                By default this is an empty dictionary, which will summarize the measure passed in `prevalence_marker_name` using the `measure_summary_float` function.
                                                                If you want to use one function for all measure values, you can just supply { "all": function }.
                                                                If a measure is supplied without a callable function, `measure_summary_float` will be used by default.
    Returns:
        Returns a dataframe with post-processed metrics for the given input
    """
    column_names = df.columns
    tmp = df.to_numpy()

    measure_column_loc = column_names.get_loc(measure_column_name)
    year_column_loc = column_names.get_loc(year_column_name)
    age_start_loc = column_names.get_loc(age_start_column_name)
    age_end_loc = column_names.get_loc(age_end_column_name)
    draws_loc = [column_names.get_loc(name) for name in draw_names]

    # Data manipulation
    # Making sure we start the calculations from where we want
    post_processing_start_mask = tmp[:, year_column_loc].astype(float) >= post_processing_start_time
    tmp_for_calc = tmp[post_processing_start_mask, :]

    # Selecting only the rows that have the given prevalence measure
    mf_prev_mask = tmp_for_calc[:, measure_column_loc] == prevalence_marker_name
    # This line will give us a new metrix that is N rows (years) x M columns (draws), each containing the mf prevalence
    # values for the given cell (n, m)
    mf_prev_vals = tmp_for_calc[mf_prev_mask, :][:, draws_loc].astype(float)

    # Creating a mask to find which cell values are under the threshold (NxM matrix with True or False)
    mf_under_1_mask = mf_prev_vals < threshold

    # Finding the mean of True/False values in a row, which returns the proportion of draws under the threshold
    # Results should be a Nx1 matrix (each row is the proportion for a year)
    prob_under_1_mfp = np.mean(mf_under_1_mask, axis=1)
    num_rows = len(prob_under_1_mfp)
    nan_array = np.full(num_rows, np.nan)
    none_array = np.full(num_rows, None)
    # Creating an output matrix for the overall probability values
    prob_under_1_mfp_output = np.column_stack(
        (
            tmp_for_calc[mf_prev_mask, year_column_loc], # year
            nan_array, # age_start
            nan_array, # age_end
            np.full(num_rows, "prob_under_1_mfp"),  # measure
            prob_under_1_mfp, # mean
            none_array, # 2.5_percentile
            none_array, # 5_percentile
            none_array, # 10_percentile
            none_array, # 25_percentile
            none_array, # 50_percentile
            none_array, # 75_percentile
            none_array, # 90_percentile
            none_array, # 95_percentile
            none_array, # 97.5_percentile
            none_array, # standard deviation
            none_array, # median
        )
    )

    # find the rows where the proportion is >= pct_runs_under_threshold, select the top row as the first index
    # todo: verify/dynamically select the lowest year
    indeces_of_90_under_1_mfp = np.where(prob_under_1_mfp >= pct_runs_under_threshold)[0]
    year_of_90_under_1_mfp = None
    if indeces_of_90_under_1_mfp.size > 0:
        year_of_90_under_1_mfp = tmp_for_calc[mf_prev_mask, :][
            indeces_of_90_under_1_mfp[0], year_column_loc
        ]
    year_90_under_1_mfp_output = np.column_stack(
        (
            "", # year
            np.nan, # age_start
            np.nan, # age_end
            "year_of_90_under_1_mfp", # measure
            year_of_90_under_1_mfp, # mean
            None, # 2.5_percentile
            None, # 5_percentile
            None, # 10_percentile
            None, # 25_percentile
            None, # 50_percentile
            None, # 75_percentile
            None, # 90_percentile
            None, # 95_percentile
            None, # 97.5_percentile
            None, # standard deviation
            None, # median
        )
    )

    # Calculating the year where the avg across all the runs has a prevalence < the threshold
    yearly_avg_mfp = np.mean(mf_prev_vals, axis=1)
    indeces_of_1_mfp_avg = np.where(yearly_avg_mfp < threshold)[0]
    year_of_1_mfp_avg = None
    if indeces_of_1_mfp_avg.size > 0:
        year_of_1_mfp_avg = tmp_for_calc[mf_prev_mask, :][indeces_of_1_mfp_avg[0], year_column_loc]
    year_under_1_avg_mfp_output = np.column_stack(
        (
            "", # year
            np.nan, # age_start
            np.nan, # age_end
            "year_of_1_mfp_avg",
            year_of_1_mfp_avg, # mean
            None, # 2.5_percentile
            None, # 5_percentile
            None, # 10_percentile
            None, # 25_percentile
            None, # 50_percentile
            None, # 75_percentile
            None, # 90_percentile
            None, # 95_percentile
            None, # 97.5_percentile
            None, # standard deviation
            None, # median
        )
    )

    # Summarizing all other outputs

    measure_summaries_output = None
    # If the map is empty or doesn't contain the for the prevalence marker, add it to the list of measure summaries
    # to calculate using the default summary function
    if (len(measure_summary_map) == 0) | (prevalence_marker_name not in measure_summary_map.keys()):
        measure_summary_map[prevalence_marker_name] = measure_summary_float

    # if they don't specify "all" in the summary map, loop through each measure key and calculate the summary for it
    # independently, using either the provided function or the default one
    if 'all' not in measure_summary_map:
        for key, value in measure_summary_map.items():
            tmp_measure_mask = tmp[:, measure_column_loc] == key
            func_to_summarize = measure_summary_float
            if callable(value):
                func_to_summarize = value

            if type(measure_summaries_output) == type(None):
                measure_summaries_output = func_to_summarize(tmp[tmp_measure_mask, :], [year_column_loc, age_start_loc, age_end_loc, measure_column_loc], draws_loc)
            else:
                measure_summaries_output = np.row_stack((
                    measure_summaries_output, 
                    func_to_summarize(tmp[tmp_measure_mask, :], [year_column_loc, age_start_loc, age_end_loc, measure_column_loc], draws_loc)
                    ))
    # if they specify "all" in the summary map, calculate the summary for all measures using either the provided function
    # or the default one
    else:
        if callable(measure_summary_map['all']):
            measure_summaries_output = measure_summary_map['all'](tmp, [year_column_loc, age_start_loc, age_end_loc, measure_column_loc], draws_loc)
        else:
            measure_summaries_output = measure_summary_float(tmp, [year_column_loc, age_start_loc, age_end_loc, measure_column_loc], draws_loc)
    
    # combine all the outputs together
    output = np.row_stack(
        (
            measure_summaries_output,
            # probability of 1% mf_prev for each year
            prob_under_1_mfp_output,
            # year that the avg mfp is < 1%
            year_under_1_avg_mfp_output,
            # year that the 90% of runs have < 1% mfp
            year_90_under_1_mfp_output,
        )
    )

    # add the necessary descriptor columns
    descriptor_output = np.column_stack(
        (
            np.full(output.shape[0], iuName),
            # extracting country code
            np.full(output.shape[0], iuName[:3]),
            np.full(output.shape[0], scenario),
            output,
        )
    )

    # return a dataframe
    return pd.DataFrame(
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
    )