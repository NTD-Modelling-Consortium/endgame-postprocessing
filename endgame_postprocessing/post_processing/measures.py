import numpy as np
from .constants import PERCENTILES_TO_CALC

def _extract_percentiles(
    percentiles_dict: dict[str, list[int]], percentile_names: list[str]
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
    percentiles_dict: dict[str, list[int]],
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
    percentile_stack = _extract_percentiles(percentiles_dict, percentile_name_order)

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
        The year in which the threshold was reached, or -1 if it wasn't.
    """
    indeces_of_meeting_threshold = np.where(
        comparitor_function(comparison_prevalence_values, threshold)
    )[0]
    if indeces_of_meeting_threshold.size > 0:
        return year_ids[indeces_of_meeting_threshold[0]]
    return -1

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
