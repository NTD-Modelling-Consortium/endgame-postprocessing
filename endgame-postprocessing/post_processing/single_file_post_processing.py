import numpy as np
import pandas as pd
from .constants import PERCENTILES_TO_CALC
from .measures import (
    build_summary,
    _calc_prob_under_threshold,
    _find_year_reaching_threshold,
    measure_summary_float
)

def validate_measure_map(
        measure_summary_map: dict,
        prevalence_measure_name: str
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
        if (func is None):
            measure_summary_map[key] = measure_summary_float
        elif not(callable(func)):
            raise ValueError(f"Value for key '{key}'" +
                             "in measure_summary_map is not callable and not None")
    return measure_summary_map

def _filter_out_old_data(
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

def _calculate_probabilities_and_thresholds(
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

    prob_prevalence_under_threshold = _calc_prob_under_threshold(
        prevalence_vals, threshold
    )
    num_rows = len(prob_prevalence_under_threshold)
    none_array = np.full(num_rows, None)

    # Creating an output matrix for the overall probability values
    prob_under_threshold_prevalence_output = build_summary(
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
    year_of_pct_runs_under_threshold = _find_year_reaching_threshold(
        prob_prevalence_under_threshold,
        pct_runs_under_threshold,
        filtered_model_outputs[prevalence_mask, year_column_loc],
        np.greater_equal,
    )

    year_of_pct_runs_under_threshold_output = build_summary(
        year_id=[""],
        age_start=[None],
        age_end=[None],
        measure_name=["year_of_pct_runs_under_threshold"],
        mean=[year_of_pct_runs_under_threshold],
        percentiles_dict={k: [None] for k in PERCENTILES_TO_CALC},
        percentile_name_order=PERCENTILES_TO_CALC,
        standard_deviation=[None],
        median=[None],
    )

    # Calculating the year where the avg across all the runs has a prevalence < the threshold
    year_of_threshold_prevalence_avg = _find_year_reaching_threshold(
        np.mean(prevalence_vals, axis=1),
        threshold,
        filtered_model_outputs[prevalence_mask, year_column_loc],
        np.less,
    )
    year_of_threshold_prevalence_avg_output = build_summary(
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
            # probability of X% prevalence for each year
            prob_under_threshold_prevalence_output,
            # year that the avg prevalence is < X%
            year_of_threshold_prevalence_avg_output,
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
    measure_summary_map = validate_measure_map(measure_summary_map, prevalence_marker_name)

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
        - (for each year) the probability that < X% prevalence was reached across all draws
        - the year at which < X% prevalence is reached, calculated using the average prevalence
        across all runs
        - the year at which Y% of runs reach < X% prevalence is reached

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
    filtered_model_outputs = _filter_out_old_data(
        raw_model_outputs.to_numpy(), year_column_loc, post_processing_start_time
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
