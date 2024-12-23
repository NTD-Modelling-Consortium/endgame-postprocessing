import numpy as np
import pandas as pd
from .constants import (
    AGE_END_COLUMN_NAME,
    AGE_START_COLUMN_NAME,
    DRAW_COLUMNN_NAME_START,
    FINAL_COLUMNS,
    MEASURE_COLUMN_NAME,
    PERCENTILES_TO_CALC,
    YEAR_COLUMN_NAME,
    DEFAULT_PREVALENCE_MEASURE_NAME,
    PROB_UNDER_THRESHOLD_MEASURE_NAME
)
from .measures import (
    build_summary,
    calc_prob_under_threshold,
    find_year_reaching_threshold,
    measure_summary_float,
)


def validate_measure_map(
    measure_summary_map: dict, prevalence_measure_name: str
) -> dict:
    """
    Returns a validated map of measures to summarize, that has, at minimum the prevalence marker.
    None values are replaced with the measure_summary_float.
    Existance of non-callable functions will throw an error
    Args:
        measure_summary_map (dict): A dictionary of measure_name -> callable/None
        prevalence_marker_name (str): The name of the prevalence measure in your dataset
    Returns:
        A map of measure names -> summary function with all keys having a callable function
    """
    if measure_summary_map is None:
        return {prevalence_measure_name: measure_summary_float}
    elif prevalence_measure_name not in measure_summary_map.keys():
        measure_summary_map[prevalence_measure_name] = measure_summary_float
    for key, func in measure_summary_map.items():
        if func is None:
            measure_summary_map[key] = measure_summary_float
        elif not (callable(func)):
            raise ValueError(
                f"Value for key '{key}'"
                + "in measure_summary_map is not callable and not None"
            )
    return measure_summary_map


def _filter_out_old_data(
    raw_model_outputs: np.ndarray,
    year_column_loc: int,
    post_processing_start_time: int,
    post_processing_end_time: int
) -> np.ndarray:
    """
    Returns a filtered version of the input data where the time is after a certain start year.

    Args:
        raw_model_outputs (np.ndarray): The original model data, in the format of a 2D array.
        year_column_loc (int): The location of the year_id column.
        post_processing_start_time (int): The start time after which you want the data to be used.

    Returns:
        A filtered 2D matrix with the output data.
    """
    # Making sure we start the calculations from where we want
    start_mask = np.logical_and(
        raw_model_outputs[:, year_column_loc].astype(float)
        >= post_processing_start_time,
        raw_model_outputs[:, year_column_loc].astype(float)
        <= post_processing_end_time
    )
    return raw_model_outputs[start_mask, :]


def _calculate_probabilities_and_thresholds(
    filtered_model_outputs: np.array,
    year_column_loc: int,
    measure_column_loc: int,
    age_start_column_loc: int,
    age_end_column_loc: int,
    draws_loc: list[int],
    prevalence_marker_name: str,
    threshold: float,
    pct_runs_under_threshold: list[float],
) -> np.ndarray:
    """
    A helper function that takes in filtered model outputs and generatres:
        - (for each year) the probability that < X% prevalence was reached across all draws
        - the year at which < X% prevalence is reached, calculated using the average prevalence
                across all runs
        - the year at which Y% of runs reach < X% prevalence is reached

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
        pct_runs_under_threshold (list[float]): the percentages of runs that should reach the given
                                            threshold (pct_runs >= pct_runs_under_threshold).
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
    prob_under_threshold_prevalence_output = build_summary(
        year_id=filtered_model_outputs[prevalence_mask, year_column_loc],
        age_start=filtered_model_outputs[prevalence_mask, age_start_column_loc],
        age_end=filtered_model_outputs[prevalence_mask, age_end_column_loc],
        measure_name=np.full(num_rows, PROB_UNDER_THRESHOLD_MEASURE_NAME),
        mean=prob_prevalence_under_threshold,
        percentiles_dict={k: none_array for k in PERCENTILES_TO_CALC},
        percentile_name_order=PERCENTILES_TO_CALC,
        standard_deviation=none_array,
        median=none_array,
    )

    # find the rows where the proportion is >= pct_runs_under_threshold, select the top row as the
    # first index
    # todo: verify/dynamically select the lowest year
    years_of_pct_runs_under_threshold = []
    for pct in pct_runs_under_threshold:
        years_of_pct_runs_under_threshold.append(
            find_year_reaching_threshold(
                prob_prevalence_under_threshold,
                pct,
                filtered_model_outputs[prevalence_mask, year_column_loc],
                np.greater_equal,
            )
        )

    none_array = np.full(len(years_of_pct_runs_under_threshold), None)
    year_of_pct_runs_under_threshold_output = build_summary(
        year_id=none_array,
        age_start=none_array,
        age_end=none_array,
        measure_name=[
            f"year_of_{int(pct*100)}pct_runs_under_threshold"
            for pct in pct_runs_under_threshold
        ],
        mean=years_of_pct_runs_under_threshold,
        percentiles_dict={k: none_array for k in PERCENTILES_TO_CALC},
        percentile_name_order=PERCENTILES_TO_CALC,
        standard_deviation=none_array,
        median=none_array,
    )

    return np.row_stack(
        (
            # probability of X% prevalence for each year
            prob_under_threshold_prevalence_output,
            # year that the Y% of runs have < X% prev
            year_of_pct_runs_under_threshold_output,
        )
    )


def _summarize_measures(
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
    measure_summary_map = validate_measure_map(
        measure_summary_map, prevalence_marker_name
    )

    # loop through each measure key and calculate the summary for it independently, using either the
    # provided function or the default one
    for key, func_to_summarize in measure_summary_map.items():
        tmp_measure_mask = raw_output_data[:, measure_column_loc] == key
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
    num_draws: int = 200,
    prevalence_marker_name: str = "prevalence",
    post_processing_start_time: int = 1970,
    post_processing_end_time: int = 2041,
    threshold: float = 0.01,
    pct_runs_under_threshold: list[float] = [0.90],
    measure_summary_map: dict = None,
) -> pd.DataFrame:
    """
    Takes in non-age-grouped model outputs and generates a summarized output file that summarizes
    requested measure values into into mean, median, standard deviation, 2.5, 5, 10, 25, 50, 75,
    90, 95, and 97.5 percentiles.
    It also calculates:
        - (for each year) the probability that < X% prevalence was reached across all draws
        - the year at which < X% prevalence is reached, calculated using the average prevalence
        across all runs
        - the year at which Y% of runs reach < X% prevalence is reached

    Args:
        raw_model_outputs (pd.DataFrame): The raw data output in the form of a dataframe from
                                            multiple runs of the model.
        iuName (str): A name to define the parameters used for the model, typically the name of the
                                            IU being simulated.
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

    measure_column_loc = column_names.get_loc(MEASURE_COLUMN_NAME)
    year_column_loc = column_names.get_loc(YEAR_COLUMN_NAME)
    age_start_column_loc = column_names.get_loc(AGE_START_COLUMN_NAME)
    age_end_column_loc = column_names.get_loc(AGE_END_COLUMN_NAME)
    draw_names = [f"{DRAW_COLUMNN_NAME_START}{i}" for i in range(0, num_draws)]
    draws_loc = [column_names.get_loc(name) for name in draw_names]

    # Making sure we start the calculations from where we want
    filtered_model_outputs = _filter_out_old_data(
        raw_model_outputs.to_numpy(),
        year_column_loc,
        post_processing_start_time,
        post_processing_end_time,
    )
    probabilities_and_threshold_outputs = _calculate_probabilities_and_thresholds(
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

    summarized_measure_outputs = _summarize_measures(
        raw_model_outputs.to_numpy(),
        year_column_loc,
        measure_column_loc,
        age_start_column_loc,
        age_end_column_loc,
        draws_loc,
        prevalence_marker_name,
        measure_summary_map,
    )

    summarized_measure_outputs[:, 3] = np.where(
        summarized_measure_outputs[:, 3] == prevalence_marker_name,
        DEFAULT_PREVALENCE_MEASURE_NAME,
        summarized_measure_outputs[:, 3]
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
    return pd.DataFrame(descriptor_output, columns=FINAL_COLUMNS)
