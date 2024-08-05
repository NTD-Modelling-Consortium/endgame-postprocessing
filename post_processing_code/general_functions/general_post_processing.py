import numpy as np
import pandas as pd
import os
from post_processing_code.constants import PERCENTILES_TO_CALC
from typing import Generator, Tuple


# todo: have the post processing be unique functions to call, or use a class with attributes


def custom_progress_bar_update(progress_bar, curr_index: int, total: int):
    """
    Custom update for the progress bar to work with the `post_process_file_generator` below.

    Args:
        progress_bar: the progress bar object
        curr_index (int): The current index that the progress bar should be on.
        total (int): The total number of iterations that should occur.
    """
    if progress_bar.total != total:
        progress_bar.total = total
        progress_bar.refresh()
    if progress_bar.n != curr_index & curr_index <= progress_bar.total:
        progress_bar.n = curr_index
        progress_bar.refresh()


def post_process_file_generator(
    file_directory: str,
    end_of_file: str = ".csv",
) -> Generator[Tuple[int, int, str, str, str, str], None, None]:
    """
    Returns a generator for files in a given directory, only returning files that end
    with a certain string.

    Args:
        file_directory (str): The name of the file directory all the files are in. Should be in the
                                format file_directory/scenario/country/iu/output_file.csv.
        end_of_file (str): A substring that defines the files to be processed. Default is ".csv".

    Returns:
        Yields a generator, which is a tuple, of form (scenario_index, total_scenarios, scenario,
        country, iu, full_file_path).
    """
    total_scenarios = len(os.listdir(file_directory))
    for scenario_index, scenario in enumerate(os.listdir(file_directory)):
        scenario_dir_path = os.path.join(file_directory, scenario)
        if not (os.path.isdir(scenario_dir_path)):
            continue
        for country in os.listdir(scenario_dir_path):
            country_dir_path = os.path.join(file_directory, scenario, country)
            if not (os.path.isdir(country_dir_path)):
                continue
            for iu in os.listdir(country_dir_path):
                iu_dir_path = os.path.join(file_directory, scenario, country, iu)
                if not (os.path.isdir(iu_dir_path)):
                    continue
                for output_file in os.listdir(iu_dir_path):
                    if output_file.endswith(end_of_file):
                        yield (
                            scenario_index,
                            total_scenarios,
                            scenario,
                            country,
                            iu,
                            os.path.join(
                                file_directory, scenario, country, iu, output_file
                            ),
                        )


def measure_summary_float(
    data_to_summarize: np.ndarray,
    year_id_loc: int,
    measure_column_loc: int,
    age_start_loc: int,
    age_end_loc: int,
    draws_loc: list[int],
) -> np.ndarray:
    """
    The default summary calculation for a float measure, including calculation of mean, median, std,
    and percentiles.

    Args:
        data_to_summarize (np.ndarray): A 2D numpy matrix that contains the data to be summarized.
        year_id_loc (int): the location of the year_id column.
        age_start_loc (int): the location of the age_start column.
        age_end_loc (int): the location of the age_end column.
        measure_column_loc (int): the location of the measure column.
        draws_loc (list[int]): the locations of the draw columns.

    Returns:
        Returns a summarized (using build_summary) 2D matrix where each column represents a unique
        summarized value.
    """
    values = data_to_summarize[:, draws_loc].astype(float)
    return build_summary(
        year_id=data_to_summarize[:, year_id_loc],
        age_start=data_to_summarize[:, age_start_loc],
        age_end=data_to_summarize[:, age_end_loc],
        measure_name=data_to_summarize[:, measure_column_loc],
        mean=np.mean(values, axis=1),
        percentiles_dict={
            k: np.percentile(values, k, axis=1) for k in PERCENTILES_TO_CALC
        },
        percentile_name_order=PERCENTILES_TO_CALC,
        standard_deviation=np.std(values, axis=1),
        median=np.median(values, axis=1),
    )


def extract_percentiles(
    percentiles_dict: dict[str : list[int]], percentile_names: list[str]
) -> np.ndarray:
    """
    Helper function to extract percentiles from a dictionary of percentile names to a list of values
    into a 2d matrix. The columns denote each percentile (i.e col 0 = 2.5 percentile, col 1 = 5th
    percentile, etc.)

    Args:
        percentiles_dict (dict[str: list[int]]): A dictionary of percentile_name: a list of
                                                percentile calculations with that name.
        percentile_names (list[str]): A list of percentile names in the order in which the columns
                                        should appear.

    Returns:
        Returns a 2D matrix where columns denote each percentile (i.e col 0 = 2.5 percentile,
        col 1 = 5th percentile, etc.)
    """
    assert len(percentile_names) > 0 and len(percentile_names) <= len(
        percentiles_dict.keys()
    )
    output_array = np.empty(
        [len(percentiles_dict[percentile_names[0]]), len(percentile_names)]
    )
    for index, percentile_name in enumerate(percentile_names):
        percentile_list = percentiles_dict[percentile_name]
        if len(percentile_list) != output_array.shape[0]:
            raise ValueError(
                "Percentile size is not consistant, expected size"
                + f"{output_array.shape[0]}, but got size {len(percentile_list)}"
                + f"for key {percentile_name}"
            )
        output_array[:, index] = percentiles_dict[percentile_name]
    return output_array


def build_summary(
    year_id: list[int],
    age_start: list[int],
    age_end: list[int],
    measure_name: list[str],
    mean: list[int],
    percentiles_dict: dict[str : list[int]],
    percentile_name_order: list[str],
    standard_deviation: list[int],
    median: list[int],
) -> np.ndarray:
    """
    Helper function to build a standardized summary "block" based on the input. All inputs should be
    of the same length.

    Args:
        year_id (list[int]): A list of year_ids.
        age_start (list[int]): A list of age_start values.
        age_end (list[int]): A list of age_end values.
        measure_name (list[str]): A list of measure names.
        mean (list[int]): A list of the mean of the measure
        percentiles_dict (list[int]): A dictionary of percentile_name: a list of percentile
                                    calculations with that name.
        percentile_name_order (list[int]): A list of percentile names in the order in which the
                                    columns should appear.
        standard_deviation (list[int]): A list of the standard deviation of the measure
        median (list[int]):  A list of the medians of the measure

    Returns:
        Returns a 2D matrix where columns denote each value passed in. (i.e col 0 = year_id,
        col 1 = age_start, etc.)
    """
    if not (
        len(year_id)
        == len(age_start)
        == len(age_end)
        == len(measure_name)
        == len(mean)
        == len(standard_deviation)
        == len(median)
    ):
        raise ValueError("Parameters are not of the correct length")
    percentile_stack = extract_percentiles(percentiles_dict, percentile_name_order)

    if percentile_stack.shape[0] != len(year_id):
        raise ValueError("Percentile parameters are not of the correct shape")
    return np.column_stack(
        (
            year_id,
            age_start,
            age_end,
            measure_name,
            mean,
            percentile_stack,
            standard_deviation,
            median,
        )
    )


def calc_prob_under_threshold(
    prevalence_vals: np.ndarray,
    threshold: float,
) -> np.ndarray:
    """
    Calculates the probability of reaching a given threshold across all runs for a year

    Args:
        prevalence_vals (np.ndarray): A 2D matrix where columns are different runs, and rows are
                                        different years.
        threshold (float): The number at which a threshold is considered to be passed.

    Returns:
        A matrix of shape [length of the number of years in the input, 1] where the values are the
        probability of that year reaching the threshold.
    """
    # Creating a mask to find which cell values are under the threshold
    # (NxM matrix with True or False)
    prevalence_under_threshold_mask = prevalence_vals < threshold

    # Finding the mean of True/False values in a row, which returns the proportion of draws
    # under the threshold. Results should be a Nx1 matrix (each row is the proportion for a year)
    return np.mean(prevalence_under_threshold_mask, axis=1)


def find_year_reaching_threshold(
    comparison_prevalence_values: list[list[float]],
    threshold: float,
    year_ids: list[float],
    comparitor_function=np.less,
) -> float:
    """
    Calculates the year in which a threshold was reached.

    Args:
        comparison_prevalence_values (list[list[float]]): an array of shape [# of years, 1]
        threshold (float): The threshold to compare to
        year_ids (list[float]): A list of the related year_ids to the comparison_prevalence_values
        comparitor_function (function): The numpy comparator function to be used.
                                            By default this is np.less.

    Returns:
        The year in which the threshold was reached, or NaN if it wasn't.
    """
    indeces_of_meeting_threshold = np.where(
        comparitor_function(comparison_prevalence_values, threshold)
    )[0]
    if indeces_of_meeting_threshold.size > 0:
        return year_ids[indeces_of_meeting_threshold[0]]
    return np.nan


def filter_out_old_data(
    raw_model_outputs: np.ndarray,
    year_column_loc: int,
    post_processing_start_time: float,
) -> np.ndarray:
    """
    Returns a filtered version of the input data where the time is after a certain start year.

    Args:
        raw_model_outputs (np.ndarray): The original model data, in the format of a 2D array.
        year_column_loc (int): The location of the year_id column.
        post_processing_start_time (float): The start time after which you want the data to be used.

    Returns:
        A filtered 2D matrix with the output data.
    """
    # Making sure we start the calculations from where we want
    start_mask = (
        raw_model_outputs[:, year_column_loc].astype(float)
        >= post_processing_start_time
    )
    return raw_model_outputs[start_mask, :]


def calculate_probabilities_and_thresholds(
    filtered_model_outputs: np.array,
    year_column_loc: int,
    measure_column_loc: int,
    age_start_column_loc: int,
    age_end_column_loc: int,
    draws_loc: list[int],
    prevalence_marker_name: str,
    threshold: float,
    pct_runs_under_threshold: float,
) -> np.ndarray:
    """
    A helper function that takes in filtered model outputs and generatres:
        - (for each year) the probability that < 1% mf prevalence was reached across all draws
        - the year at which < 1% mf prevalence is reached, calculated using the average prevalence
                across all runs
        - the year at which 90% of runs reach < 1% mf prevalence is reached

    Args:
        filtered_model_outputs (pd.DataFrame): The filtered data output in the form of a dataframe
                                                from multiple runs of the model.
        year_column_loc (int): Location of the year column.
        measure_column_loc (int): Location of the measure column.
        age_start_column_loc (int): Location of the age_start column.
        age_end_column_loc (int): Location of the age_end column.
        draws_loc list(int): Location of the draw columns.
        prevalence_marker_name (str): The name of the prevalence measure that is used to
                                        compare with the threhsold.
        threshold (float): the value of the threshold we compare prevalence values
                            to (value < threhsold).
        pct_runs_under_threshold (float): the percent of runs that should reach the given threshold
                                            (pct_runs >= pct_runs_under_threshold).
    Returns:
        Returns a 2D Matrix with the post-processed metrics for the given input
    """
    # Selecting only the rows that have the given prevalence measure
    prevalence_mask = (
        filtered_model_outputs[:, measure_column_loc] == prevalence_marker_name
    )
    # This line will give us a new metrix that is N rows (years) x M columns (draws), each
    # containing the prevalence values for the given cell (n, m)
    prevalence_vals = filtered_model_outputs[prevalence_mask, :][:, draws_loc].astype(
        float
    )

    prob_prevalence_under_threshold = calc_prob_under_threshold(
        prevalence_vals, threshold
    )
    num_rows = len(prob_prevalence_under_threshold)
    none_array = np.full(num_rows, None)

    # Creating an output matrix for the overall probability values
    prob_under_1_prevalence_output = build_summary(
        year_id=filtered_model_outputs[prevalence_mask, year_column_loc],
        age_start=filtered_model_outputs[prevalence_mask, age_start_column_loc],
        age_end=filtered_model_outputs[prevalence_mask, age_end_column_loc],
        measure_name=np.full(num_rows, "prob_under_1_prevalence"),
        mean=prob_prevalence_under_threshold,
        percentiles_dict={k: none_array for k in PERCENTILES_TO_CALC},
        percentile_name_order=PERCENTILES_TO_CALC,
        standard_deviation=none_array,
        median=none_array,
    )

    # find the rows where the proportion is >= pct_runs_under_threshold, select the top row as the
    # first index
    # todo: verify/dynamically select the lowest year
    year_of_90_under_threshold = find_year_reaching_threshold(
        prob_prevalence_under_threshold,
        pct_runs_under_threshold,
        filtered_model_outputs[prevalence_mask, year_column_loc],
        np.greater_equal,
    )

    year_90_under_1_prevalence_output = build_summary(
        year_id=[""],
        age_start=[None],
        age_end=[None],
        measure_name=["year_of_90_under_threshold"],
        mean=[year_of_90_under_threshold],
        percentiles_dict={k: [None] for k in PERCENTILES_TO_CALC},
        percentile_name_order=PERCENTILES_TO_CALC,
        standard_deviation=[None],
        median=[None],
    )

    # Calculating the year where the avg across all the runs has a prevalence < the threshold
    year_of_threshold_prevalence_avg = find_year_reaching_threshold(
        np.mean(prevalence_vals, axis=1),
        threshold,
        filtered_model_outputs[prevalence_mask, year_column_loc],
        np.less,
    )
    year_under_1_avg_prevalence_output = build_summary(
        year_id=[""],
        age_start=[None],
        age_end=[None],
        measure_name=["year_of_threshold_prevalence_avg"],
        mean=[year_of_threshold_prevalence_avg],
        percentiles_dict={k: [None] for k in PERCENTILES_TO_CALC},
        percentile_name_order=PERCENTILES_TO_CALC,
        standard_deviation=[None],
        median=[None],
    )
    return np.row_stack(
        (
            # probability of 1% prevalence for each year
            prob_under_1_prevalence_output,
            # year that the avg prevalence is < 1%
            year_under_1_avg_prevalence_output,
            # year that the 90% of runs have < 1% mfp
            year_90_under_1_prevalence_output,
        )
    )


def summarize_measures(
    raw_output_data: np.array,
    year_column_loc: int,
    measure_column_loc: int,
    age_start_column_loc: int,
    age_end_column_loc: int,
    draws_loc: list[int],
    prevalence_marker_name: str,
    measure_summary_map: dict,
) -> np.ndarray:
    """
    A helper function that takes in raw model outputs and summarizes requested measure values into
    into mean, median, standard deviation, 2.5, 5, 10, 25, 50, 75, 90, 95, and 97.5 percentiles.

    Args:
        raw_output_data (pd.DataFrame): The raw data output in the form of a dataframe from multiple
                                        runs of the model.
        year_column_loc (int): Location of the year column.
        measure_column_loc (int): Location of the measure column.
        age_start_column_loc (int): Location of the age_start column.
        age_end_column_loc (int): Location of the age_end column.
        draws_loc list(int): Location of the draw columns.
        prevalence_marker_name (str): The name of the prevalence measure that is used to
                                        compare with the threhsold.
        measure_summary_map (dict[str: function]): a map of measure names to a summary calculation
                                                functions. This will be used to calculate the mean,
                                                median, std, and percentiles.
                                                By default it is `None`, which will summarize the
                                                measure passed in `prevalence_marker_name` using
                                                the `measure_summary_float` function.
                                                If a measure is supplied without a callable function
                                                `measure_summary_float` will be used by default.
    Returns:
        Returns a 2D Matrix with the post-processed metrics for the given input
    """
    # Summarizing all other outputs
    measure_summaries_output = None
    # If the map is empty or doesn't contain the for the prevalence marker, add it to the list of
    # measure summaries to calculate using the default summary function
    if (measure_summary_map is None) | (
        prevalence_marker_name not in measure_summary_map.keys()
    ):
        measure_summary_map[prevalence_marker_name] = measure_summary_float
    # replacing all `None` with the default function
    measure_summary_map = {
        key: (func if func is not None else measure_summary_float)
        for key, func in measure_summary_map.items()
    }

    # loop through each measure key and calculate the summary for it independently, using either the
    # provided function or the default one
    for key, func_to_summarize in measure_summary_map.items():
        tmp_measure_mask = raw_output_data[:, measure_column_loc] == key
        if not (callable(func_to_summarize)):
            raise ValueError(
                f"Value for key {key} in measure_summary_map is not a callable."
                + "function. Please provide a function, or set the value to None to "
                + "use the default function"
            )

        if measure_summaries_output is None:
            measure_summaries_output = func_to_summarize(
                raw_output_data[tmp_measure_mask, :],
                year_column_loc,
                measure_column_loc,
                age_start_column_loc,
                age_end_column_loc,
                draws_loc,
            )
        else:
            measure_summaries_output = np.row_stack(
                (
                    measure_summaries_output,
                    func_to_summarize(
                        raw_output_data[tmp_measure_mask, :],
                        year_column_loc,
                        measure_column_loc,
                        age_start_column_loc,
                        age_end_column_loc,
                        draws_loc,
                    ),
                )
            )
    return measure_summaries_output


def process_single_file(
    raw_model_outputs: pd.DataFrame,
    scenario: str,
    iuName: str,
    year_column_name="year_id",
    measure_column_name="measure",
    age_start_column_name="age_start",
    age_end_column_name="age_end",
    draw_names: str = [f"draw_{i}" for i in range(0, 200)],
    prevalence_marker_name: str = "prevalence",
    post_processing_start_time: int = 1970,
    threshold: float = 0.01,
    pct_runs_under_threshold: float = 0.90,
    measure_summary_map: dict = None,
) -> pd.DataFrame:
    """
    Takes in non-age-grouped model outputs and generates a summarized output file that summarizes
    requested measure values into into mean, median, standard deviation, 2.5, 5, 10, 25, 50, 75,
    90, 95, and 97.5 percentiles.
    It also calculates:
        - (for each year) the probability that < 1% mf prevalence was reached across all draws
        - (for each year) the probability that < 0% mf prevalence was reached across all draws
        - the year at which < 1% mf prevalence is reached, calculated using the average prevalence
        across all runs
        - the year at which 90% of runs reach < 1% mf prevalence is reached

    Args:
        raw_model_outputs (pd.DataFrame): The raw data output in the form of a dataframe from
                                            multiple runs of the model.
        iuName (str): A name to define the parameters used for the model, typically the name of the
                                            IU being simulated.
        year_column_name (str): Name of the year column. Default is "year_id".
        measure_column_name (str): Name of the measure column. Default is "measure".
        age_start_column_name (str): Name of the age_start column. Default is "age_start".
        age_end_column_name (str): Name of the age_end column. Default is "age_end".
        draw_names (str): Name of the draw columns. Default is [f'draw_{i}' for i in range(0, 200)],
        prevalence_marker_name (str): The name of the prevalence measure that is used to compare
                                    with the threhsold. Default is "prevalence".
        post_processing_start_time (int): The time at which we want to start calculating the
                                            different metrics. Default is 1970.
        threshold (float): the value of the threshold we compare prevalence values to
                            (value < threhsold). Default is 0.01.
        pct_runs_under_threshold (float): the percent of runs that should reach the given threshold
                                            (pct_runs >= pct_runs_under_threshold). Default is 0.90.
        measure_summary_map (dict[str: function], optional): a map of measure names to a summary
                                calculation functions. This will be used to calculate the mean,
                                median, std, and percentiles. By default it is `None`, which will
                                summarize the measure passed in `prevalence_marker_name` using the
                                `measure_summary_float` function. If a measure is supplied without a
                                callable function, `measure_summary_float` will be used by default.
    Returns:
        Returns a dataframe with post-processed metrics for the given input
    """
    column_names = raw_model_outputs.columns

    measure_column_loc = column_names.get_loc(measure_column_name)
    year_column_loc = column_names.get_loc(year_column_name)
    age_start_column_loc = column_names.get_loc(age_start_column_name)
    age_end_column_loc = column_names.get_loc(age_end_column_name)
    draws_loc = [column_names.get_loc(name) for name in draw_names]

    # Making sure we start the calculations from where we want
    filtered_model_outputs = filter_out_old_data(
        raw_model_outputs.to_numpy(), year_column_loc, post_processing_start_time
    )
    probabilities_and_threshold_outputs = calculate_probabilities_and_thresholds(
        filtered_model_outputs,
        year_column_loc,
        measure_column_loc,
        age_start_column_loc,
        age_end_column_loc,
        draws_loc,
        prevalence_marker_name,
        threshold,
        pct_runs_under_threshold,
    )

    summarized_measure_outputs = summarize_measures(
        raw_model_outputs.to_numpy(),
        year_column_loc,
        measure_column_loc,
        age_start_column_loc,
        age_end_column_loc,
        draws_loc,
        prevalence_marker_name,
        measure_summary_map,
    )

    # combine all the outputs together
    output = np.row_stack(
        (summarized_measure_outputs, probabilities_and_threshold_outputs)
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
        ]
        + [str(p) + "_percentile" for p in PERCENTILES_TO_CALC]
        + ["standard_deviation", "median"],
    )
