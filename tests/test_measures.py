import pytest
import numpy as np
from endgame_postprocessing.post_processing.measures import (
    _extract_percentiles,
    build_summary,
    calc_prob_under_threshold,
    measure_summary_float
)
from .test_helper_functions import (
    PERCENTILES_TO_TEST,
    EXPECTED_ROWS,
    generate_test_input,
    check_if_columns_is_float
)

def test_extract_percentiles_success():
    percentiles_dict = {
        PERCENTILES_TO_TEST[0]: np.random.rand(EXPECTED_ROWS),
        PERCENTILES_TO_TEST[1]: np.random.rand(EXPECTED_ROWS),
        PERCENTILES_TO_TEST[2]: np.random.rand(EXPECTED_ROWS)
    }
    percentile_matrix = _extract_percentiles(percentiles_dict, PERCENTILES_TO_TEST)
    assert percentile_matrix.shape[0] == EXPECTED_ROWS
    assert percentile_matrix.shape[1] == len(PERCENTILES_TO_TEST)
    assert (percentile_matrix[:,0] == percentiles_dict[PERCENTILES_TO_TEST[0]]).all()
    assert (percentile_matrix[:,1] == percentiles_dict[PERCENTILES_TO_TEST[1]]).all()
    assert (percentile_matrix[:,2] == percentiles_dict[PERCENTILES_TO_TEST[2]]).all()


def test_extract_percentiles_fail():
    percentiles_dict = {
        PERCENTILES_TO_TEST[0]: np.random.rand(EXPECTED_ROWS),
        PERCENTILES_TO_TEST[1]: np.random.rand(EXPECTED_ROWS-1),
        PERCENTILES_TO_TEST[2]: np.random.rand(EXPECTED_ROWS)
    }
    with pytest.raises(ValueError):
        _extract_percentiles(percentiles_dict, PERCENTILES_TO_TEST)


def test_build_summary_success():
    min_year = 1970
    max_year = min_year + EXPECTED_ROWS
    percentiles_dict = {
        PERCENTILES_TO_TEST[0]: np.random.rand(EXPECTED_ROWS),
        PERCENTILES_TO_TEST[1]: np.random.rand(EXPECTED_ROWS),
        PERCENTILES_TO_TEST[2]: np.random.rand(EXPECTED_ROWS)
    }
    summarized_matrix = build_summary(
        year_id = np.arange(min_year, max_year),
        age_start = np.full(EXPECTED_ROWS, 5),
        age_end = np.full(EXPECTED_ROWS, 80),
        measure_name = np.full(EXPECTED_ROWS, "test_measure"),
        mean = np.random.rand(EXPECTED_ROWS),
        percentiles_dict = percentiles_dict,
        percentile_name_order = PERCENTILES_TO_TEST,
        standard_deviation = np.random.rand(EXPECTED_ROWS),
        median = np.random.rand(EXPECTED_ROWS)
    )
    assert summarized_matrix.shape[0] == EXPECTED_ROWS
    assert summarized_matrix.shape[1] == 10
    assert check_if_columns_is_float(
        summarized_matrix,
        [3])

def test_build_summary_fail():
    min_year = 1970
    max_year = min_year + EXPECTED_ROWS
    percentiles_dict = {
        PERCENTILES_TO_TEST[0]: np.random.rand(EXPECTED_ROWS),
        PERCENTILES_TO_TEST[1]: np.random.rand(EXPECTED_ROWS),
        PERCENTILES_TO_TEST[2]: np.random.rand(EXPECTED_ROWS)
    }
    with pytest.raises(ValueError):
        build_summary(
            year_id = np.arange(min_year, max_year),
            age_start = np.full(EXPECTED_ROWS - 1, 5),
            age_end = np.full(EXPECTED_ROWS, 80),
            measure_name = np.full(EXPECTED_ROWS, "test_measure"),
            mean = np.random.rand(EXPECTED_ROWS),
            percentiles_dict = percentiles_dict,
            percentile_name_order = PERCENTILES_TO_TEST,
            standard_deviation = np.random.rand(EXPECTED_ROWS),
            median = np.random.rand(EXPECTED_ROWS)
        )



def test_calc_prob_under_threshold():
    prev_values = np.column_stack((
        np.full(EXPECTED_ROWS, 0.2),
        np.full(EXPECTED_ROWS, 0.05),
        np.full(EXPECTED_ROWS, 0.03),
        np.full(EXPECTED_ROWS, 0.3)
    ))
    prob_outputs = calc_prob_under_threshold(prev_values, 0.1)
    assert (prob_outputs == np.full((EXPECTED_ROWS, 1), 0.5)).all()


def test_measure_summary_float():
    test_input = generate_test_input()
    summary_output = measure_summary_float(
        data_to_summarize=test_input["input"],
        year_id_loc=test_input["year_loc"],
        measure_column_loc=test_input["measure_loc"],
        age_start_loc=test_input["age_start_loc"],
        age_end_loc=test_input["age_end_loc"],
        draws_loc=test_input["draws_loc"]
    )
    assert summary_output.shape[0] == test_input["total_rows"]
    assert summary_output.shape[1] == 16
    assert check_if_columns_is_float(
        summary_output,
        [test_input["measure_loc"]])
