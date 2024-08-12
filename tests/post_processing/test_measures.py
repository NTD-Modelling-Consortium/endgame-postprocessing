import pytest
import numpy as np
from endgame_postprocessing.post_processing import measures
from tests.test_helper_functions import (
    PERCENTILES_TO_TEST,
    EXPECTED_ROWS,
    generate_test_input,
    check_if_columns_is_float,
)


def test_extract_percentiles_success():
    percentiles_dict = {
        PERCENTILES_TO_TEST[0]: np.random.rand(EXPECTED_ROWS),
        PERCENTILES_TO_TEST[1]: np.random.rand(EXPECTED_ROWS),
        PERCENTILES_TO_TEST[2]: np.random.rand(EXPECTED_ROWS),
    }
    percentile_matrix = measures._extract_percentiles(
        percentiles_dict, PERCENTILES_TO_TEST
    )
    assert percentile_matrix.shape[0] == EXPECTED_ROWS
    assert percentile_matrix.shape[1] == len(PERCENTILES_TO_TEST)
    assert (percentile_matrix[:, 0] == percentiles_dict[PERCENTILES_TO_TEST[0]]).all()
    assert (percentile_matrix[:, 1] == percentiles_dict[PERCENTILES_TO_TEST[1]]).all()
    assert (percentile_matrix[:, 2] == percentiles_dict[PERCENTILES_TO_TEST[2]]).all()


def test_extract_percentiles_fail_uneven_rows():
    percentiles_dict = {
        PERCENTILES_TO_TEST[0]: np.random.rand(EXPECTED_ROWS),
        PERCENTILES_TO_TEST[1]: np.random.rand(EXPECTED_ROWS - 1),
        PERCENTILES_TO_TEST[2]: np.random.rand(EXPECTED_ROWS),
    }
    with pytest.raises(ValueError):
        measures._extract_percentiles(percentiles_dict, PERCENTILES_TO_TEST)


def test_extract_percentiles_fail_invalid_keys():
    percentiles_dict = {
        PERCENTILES_TO_TEST[0]: np.random.rand(EXPECTED_ROWS),
        PERCENTILES_TO_TEST[1]: np.random.rand(EXPECTED_ROWS - 1),
        PERCENTILES_TO_TEST[2]: np.random.rand(EXPECTED_ROWS),
    }
    with pytest.raises(KeyError):
        measures._extract_percentiles(percentiles_dict, [1, 2, 3])


def test_extract_percentiles_empty_dict():
    percentiles_dict = {}
    with pytest.raises(AssertionError):
        measures._extract_percentiles(percentiles_dict, [])
    with pytest.raises(AssertionError):
        measures._extract_percentiles(percentiles_dict, [PERCENTILES_TO_TEST])


def test_build_summary_success():
    min_year = 1970
    max_year = min_year + EXPECTED_ROWS
    percentiles_dict = {
        PERCENTILES_TO_TEST[0]: np.random.rand(EXPECTED_ROWS),
        PERCENTILES_TO_TEST[1]: np.random.rand(EXPECTED_ROWS),
        PERCENTILES_TO_TEST[2]: np.random.rand(EXPECTED_ROWS),
    }
    summarized_matrix = measures.build_summary(
        year_id=np.arange(min_year, max_year),
        age_start=np.full(EXPECTED_ROWS, 5),
        age_end=np.full(EXPECTED_ROWS, 80),
        measure_name=np.full(EXPECTED_ROWS, "test_measure"),
        mean=np.random.rand(EXPECTED_ROWS),
        percentiles_dict=percentiles_dict,
        percentile_name_order=PERCENTILES_TO_TEST,
        standard_deviation=np.random.rand(EXPECTED_ROWS),
        median=np.random.rand(EXPECTED_ROWS),
    )
    assert summarized_matrix.shape[0] == EXPECTED_ROWS
    assert summarized_matrix.shape[1] == 10
    assert check_if_columns_is_float(summarized_matrix, [3])


def test_build_summary_fail_uneven_rows():
    min_year = 1970
    max_year = min_year + EXPECTED_ROWS
    percentiles_dict = {
        PERCENTILES_TO_TEST[0]: np.random.rand(EXPECTED_ROWS),
        PERCENTILES_TO_TEST[1]: np.random.rand(EXPECTED_ROWS),
        PERCENTILES_TO_TEST[2]: np.random.rand(EXPECTED_ROWS),
    }
    with pytest.raises(ValueError):
        measures.build_summary(
            year_id=np.arange(min_year, max_year),
            age_start=np.full(EXPECTED_ROWS - 1, 5),
            age_end=np.full(EXPECTED_ROWS, 80),
            measure_name=np.full(EXPECTED_ROWS, "test_measure"),
            mean=np.random.rand(EXPECTED_ROWS),
            percentiles_dict=percentiles_dict,
            percentile_name_order=PERCENTILES_TO_TEST,
            standard_deviation=np.random.rand(EXPECTED_ROWS),
            median=np.random.rand(EXPECTED_ROWS),
        )

def test_build_summary_fail_uneven_rows_percentiles():
    min_year = 1970
    max_year = min_year + EXPECTED_ROWS
    percentiles_dict = {
        PERCENTILES_TO_TEST[0]: np.random.rand(EXPECTED_ROWS-1),
        PERCENTILES_TO_TEST[1]: np.random.rand(EXPECTED_ROWS-1),
        PERCENTILES_TO_TEST[2]: np.random.rand(EXPECTED_ROWS-1),
    }
    with pytest.raises(ValueError):
        measures.build_summary(
            year_id=np.arange(min_year, max_year),
            age_start=np.full(EXPECTED_ROWS, 5),
            age_end=np.full(EXPECTED_ROWS, 80),
            measure_name=np.full(EXPECTED_ROWS, "test_measure"),
            mean=np.random.rand(EXPECTED_ROWS),
            percentiles_dict=percentiles_dict,
            percentile_name_order=PERCENTILES_TO_TEST,
            standard_deviation=np.random.rand(EXPECTED_ROWS),
            median=np.random.rand(EXPECTED_ROWS),
        )


def test_build_summary_fail_no_lists():
    min_year = 1970
    percentiles_dict = {
        PERCENTILES_TO_TEST[0]: 1,
        PERCENTILES_TO_TEST[1]: 1,
        PERCENTILES_TO_TEST[2]: 1,
    }
    with pytest.raises(TypeError):
        measures.build_summary(
            year_id=min_year,
            age_start=5,
            age_end=80,
            measure_name="test_measure",
            mean=1,
            percentiles_dict=percentiles_dict,
            percentile_name_order=PERCENTILES_TO_TEST,
            standard_deviation=1,
            median=1,
        )


def test_calc_prob_under_threshold_success():
    prev_values = np.column_stack(
        (
            np.full(EXPECTED_ROWS, 0.2),
            np.full(EXPECTED_ROWS, 0.05),
            np.full(EXPECTED_ROWS, 0.03),
            np.full(EXPECTED_ROWS, 0.3),
        )
    )
    prob_outputs = measures.calc_prob_under_threshold(prev_values, 0.1)
    assert (prob_outputs == np.full((EXPECTED_ROWS, 1), 0.5)).all()


def test_calc_prob_under_threshold_empty_array():
    prev_values = np.column_stack(
        (
            [],
            [],
            [],
        )
    )
    prob_outputs = measures.calc_prob_under_threshold(prev_values, 0.1)
    assert (prob_outputs == np.array([])).all()

def test_find_year_reaching_threshold_success():
    min_year = 1970
    total_years = 40
    years = np.arange(min_year, min_year + total_years)
    prev_values = np.linspace(0.5, 0.01, total_years).reshape(total_years, 1)
    year = measures.find_year_reaching_threshold(
        comparison_prevalence_values=prev_values,
        threshold=0.1,
        year_ids=years,
        comparitor_function=np.less
    )
    assert year == years[-8]

def test_find_year_reaching_threshold_success_returns_nan():
    min_year = 1970
    total_years = 40
    years = np.arange(min_year, min_year+total_years)
    prev_values = np.linspace(0.02, 0.5, total_years).reshape(total_years, 1)
    year = measures.find_year_reaching_threshold(
        comparison_prevalence_values=prev_values,
        threshold=0.01,
        year_ids=years,
        comparitor_function=np.less
    )
    assert np.isnan(year)

def test_find_year_reaching_threshold_success_other_comparitor():
    min_year = 1970
    total_years = 40
    years = np.arange(min_year, min_year+total_years)
    prev_values = np.linspace(0.01, 0.5, total_years).reshape(total_years, 1)
    year = measures.find_year_reaching_threshold(
        comparison_prevalence_values=prev_values,
        threshold=0.4,
        year_ids=years,
        comparitor_function=np.greater
    )
    assert year == years[-8]


def test_measure_summary_float_success():
    test_input = generate_test_input()
    summary_output = measures.measure_summary_float(
        data_to_summarize=test_input["input"],
        year_id_loc=test_input["year_loc"],
        measure_column_loc=test_input["measure_loc"],
        age_start_loc=test_input["age_start_loc"],
        age_end_loc=test_input["age_end_loc"],
        draws_loc=test_input["draws_loc"],
    )
    assert summary_output.shape[0] == test_input["total_rows"]
    assert summary_output.shape[1] == 16
    assert check_if_columns_is_float(summary_output, [test_input["measure_loc"]])


def test_measure_summary_float_success_not_using_all_draws():
    test_input = generate_test_input()
    summary_output = measures.measure_summary_float(
        data_to_summarize=test_input["input"],
        year_id_loc=test_input["year_loc"],
        measure_column_loc=test_input["measure_loc"],
        age_start_loc=test_input["age_start_loc"],
        age_end_loc=test_input["age_end_loc"],
        draws_loc=test_input["draws_loc"][:5],
    )
    assert summary_output.shape[0] == test_input["total_rows"]
    assert summary_output.shape[1] == 16
    assert check_if_columns_is_float(summary_output, [test_input["measure_loc"]])


def test_measure_summary_float_fail_bad_year_loc():
    test_input = generate_test_input()
    with pytest.raises(IndexError):
        measures.measure_summary_float(
            data_to_summarize=test_input["input"],
            year_id_loc=30,
            measure_column_loc=test_input["measure_loc"],
            age_start_loc=test_input["age_start_loc"],
            age_end_loc=test_input["age_end_loc"],
            draws_loc=test_input["draws_loc"],
        )


def test_measure_summary_float_fail_string_in_input():
    num_draws = 10
    test_input = generate_test_input(num_draws=num_draws)
    test_input["input"][:, test_input["draws_loc"]] = np.full(
        (test_input["total_rows"], num_draws), "a"
    )
    with pytest.raises(ValueError):
        measures.measure_summary_float(
            data_to_summarize=test_input["input"],
            year_id_loc=30,
            measure_column_loc=test_input["measure_loc"],
            age_start_loc=test_input["age_start_loc"],
            age_end_loc=test_input["age_end_loc"],
            draws_loc=test_input["draws_loc"],
        )
