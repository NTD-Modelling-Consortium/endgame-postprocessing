import pytest
import numpy as np
from endgame_postprocessing.post_processing.measures import (
    _extract_percentiles,
    build_summary,
    calc_prob_under_threshold,
    measure_summary_float
)
from ..test_helper_functions import (
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

def test_extract_percentiles_fail_uneven_rows():
    percentiles_dict = {
        PERCENTILES_TO_TEST[0]: np.random.rand(EXPECTED_ROWS),
        PERCENTILES_TO_TEST[1]: np.random.rand(EXPECTED_ROWS-1),
        PERCENTILES_TO_TEST[2]: np.random.rand(EXPECTED_ROWS)
    }
    with pytest.raises(ValueError):
        _extract_percentiles(percentiles_dict, PERCENTILES_TO_TEST)

def test_extract_percentiles_fail_invalid_keys():
    percentiles_dict = {
        PERCENTILES_TO_TEST[0]: np.random.rand(EXPECTED_ROWS),
        PERCENTILES_TO_TEST[1]: np.random.rand(EXPECTED_ROWS-1),
        PERCENTILES_TO_TEST[2]: np.random.rand(EXPECTED_ROWS)
    }
    with pytest.raises(KeyError):
        _extract_percentiles(percentiles_dict, [1, 2, 3])

def test_extract_percentiles_empty_dict():
    percentiles_dict = {}
    with pytest.raises(AssertionError):
        _extract_percentiles(percentiles_dict, [])
    with pytest.raises(AssertionError):
        _extract_percentiles(percentiles_dict, [PERCENTILES_TO_TEST])

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

def test_build_summary_fail_uneven_rows():
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

def test_build_summary_fail_no_lists():
    min_year = 1970
    percentiles_dict = {
        PERCENTILES_TO_TEST[0]: 1,
        PERCENTILES_TO_TEST[1]: 1,
        PERCENTILES_TO_TEST[2]: 1
    }
    with pytest.raises(TypeError):
        build_summary(
            year_id = min_year,
            age_start = 5,
            age_end = 80,
            measure_name = "test_measure",
            mean = 1,
            percentiles_dict = percentiles_dict,
            percentile_name_order = PERCENTILES_TO_TEST,
            standard_deviation = 1,
            median = 1
        )

def test_calc_prob_under_threshold_success():
    prev_values = np.column_stack((
        np.full(EXPECTED_ROWS, 0.2),
        np.full(EXPECTED_ROWS, 0.05),
        np.full(EXPECTED_ROWS, 0.03),
        np.full(EXPECTED_ROWS, 0.3)
    ))
    prob_outputs = calc_prob_under_threshold(prev_values, 0.1)
    assert (prob_outputs == np.full((EXPECTED_ROWS, 1), 0.5)).all()

def test_calc_prob_under_threshold_empty_array():
    prev_values = np.column_stack((
        [],
        [],
        [],
    ))
    prob_outputs = calc_prob_under_threshold(prev_values, 0.1)
    assert (prob_outputs == np.array([])).all()

def test_measure_summary_float_success():
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

def test_measure_summary_float_success_not_using_all_draws():
    test_input = generate_test_input()
    summary_output = measure_summary_float(
        data_to_summarize=test_input["input"],
        year_id_loc=test_input["year_loc"],
        measure_column_loc=test_input["measure_loc"],
        age_start_loc=test_input["age_start_loc"],
        age_end_loc=test_input["age_end_loc"],
        draws_loc=test_input["draws_loc"][:5]
    )
    assert summary_output.shape[0] == test_input["total_rows"]
    assert summary_output.shape[1] == 16
    assert check_if_columns_is_float(
        summary_output,
        [test_input["measure_loc"]])

def test_measure_summary_float_fail_bad_year_loc():
    test_input = generate_test_input()
    with pytest.raises(IndexError):
        measure_summary_float(
            data_to_summarize=test_input["input"],
            year_id_loc=30,
            measure_column_loc=test_input["measure_loc"],
            age_start_loc=test_input["age_start_loc"],
            age_end_loc=test_input["age_end_loc"],
            draws_loc=test_input["draws_loc"]
        )

def test_measure_summary_float_fail_string_in_input():
    num_draws=10
    test_input = generate_test_input(num_draws=num_draws)
    test_input["input"][:,test_input["draws_loc"]] = np.full(
        (test_input["total_rows"], num_draws), "a"
    )
    with pytest.raises(ValueError):
        measure_summary_float(
            data_to_summarize=test_input["input"],
            year_id_loc=30,
            measure_column_loc=test_input["measure_loc"],
            age_start_loc=test_input["age_start_loc"],
            age_end_loc=test_input["age_end_loc"],
            draws_loc=test_input["draws_loc"]
        )
