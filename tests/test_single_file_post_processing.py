import pytest
import numpy as np
import pandas as pd
from post_processing.constants import (
    FINAL_COLUMNS,
    PERCENTILES_TO_CALC
)
from post_processing.single_file_post_processing import (
    validate_measure_map,
    _filter_out_old_data,
    _calculate_probabilities_and_thresholds,
    _summarize_measures,
    process_single_file
)
from post_processing.measures import (
    measure_summary_float
)

PREV_MEASURE_NAME = "tmp_prev"
BASE_COLUMNS = ["year_id", "age_start", "age_end", "measure"]

def generate_test_input(min_year=1970, max_year=2040, num_draws=10):
    total_rows = max_year - min_year
    input_matrix = np.column_stack((
        np.arange(min_year, max_year),
        np.full(total_rows, 5),
        np.full(total_rows, 80),
        np.full(total_rows, PREV_MEASURE_NAME),
        np.random.rand(total_rows, num_draws)
    ))
    return {
        "year_loc": 0,
        "age_start_loc": 1,
        "age_end_loc": 2,
        "measure_loc": 3,
        "draws_loc": np.arange(4, (4+num_draws)),
        "total_rows": total_rows,
        "input": input_matrix
    }

def generate_test_input_df(min_year=1970, max_year=2040, num_draws=10):
    matrix_input = generate_test_input(min_year, max_year, num_draws)["input"]
    return {
        "draws": ["draws_" + str(i) for i in range(num_draws)],
        "input_df": pd.DataFrame(
            matrix_input,
            columns = BASE_COLUMNS +
            ["draws_" + str(i) for i in range(num_draws)]
        )
    }

def check_if_columns_is_float(data, cols_to_ignore):
    for col in range(data.shape[1]):
        if not(np.isin(cols_to_ignore, col)).any():
            try:
                data[:, col].astype(float)
            except ValueError:
                raise AssertionError(f"Column # {col} " +
                                     f"cannot be converted to float {data[:, col]}.")
    return True

def test_validate_measure_map_blank_success():
    map_to_use = None
    validated_map = validate_measure_map(map_to_use, PREV_MEASURE_NAME)
    assert validated_map == {PREV_MEASURE_NAME: measure_summary_float}
def test_validate_measure_map_none_success():
    map_to_use = {"random_value": None}
    validated_map = validate_measure_map(map_to_use, PREV_MEASURE_NAME)
    assert validated_map == {
        "random_value": measure_summary_float,
        PREV_MEASURE_NAME: measure_summary_float
    }
def test_validate_measure_map_fail():
    map_to_use = {"random_value": "fake_function"}
    with pytest.raises(ValueError):
        validate_measure_map(map_to_use, PREV_MEASURE_NAME)

def test_filter_out_old_data_success():
    start_time = 1970
    end_time = 2040
    cut_off = 2000
    test_intput = generate_test_input(start_time, end_time)
    filtered_data = _filter_out_old_data(test_intput["input"], test_intput["year_loc"], cut_off)
    assert filtered_data.shape[0] == end_time-cut_off
    assert np.amin(filtered_data[:,0].astype(float)) >= cut_off

def test_calculate_probabilities_and_thresholds_success():
    test_intput = generate_test_input()
    prob_and_threshold_data = _calculate_probabilities_and_thresholds(
        test_intput["input"],
        test_intput["year_loc"],
        test_intput["measure_loc"],
        test_intput["age_start_loc"],
        test_intput["age_end_loc"],
        test_intput["draws_loc"],
        prevalence_marker_name=PREV_MEASURE_NAME,
        threshold=0.1,
        pct_runs_under_threshold=0.9)
    assert (prob_and_threshold_data[:, test_intput["measure_loc"]] == np.concatenate((
        np.full(test_intput["total_rows"], "prob_under_threshold_prevalence"),
        ["year_of_threshold_prevalence_avg", "year_of_pct_runs_under_threshold"]
    ))).all()
    assert prob_and_threshold_data.shape[0] == test_intput["input"].shape[0] + 2
    assert prob_and_threshold_data.shape[1] == 16
    assert check_if_columns_is_float(
        prob_and_threshold_data,
        [test_intput["year_loc"], test_intput["measure_loc"]])

def test_summarize_measures_success():
    test_intput = generate_test_input()
    summary_data = _summarize_measures(
        test_intput["input"],
        test_intput["year_loc"],
        test_intput["measure_loc"],
        test_intput["age_start_loc"],
        test_intput["age_end_loc"],
        test_intput["draws_loc"],
        prevalence_marker_name=PREV_MEASURE_NAME,
        measure_summary_map={PREV_MEASURE_NAME: measure_summary_float})
    assert summary_data.shape[0] == test_intput["input"].shape[0]
    assert summary_data.shape[1] == 16
    assert np.isin(
        PREV_MEASURE_NAME,
        summary_data[:, test_intput["measure_loc"]]
    )
    assert check_if_columns_is_float(summary_data, [test_intput["measure_loc"]])


def test_process_single_file_success():
    start_year = 1970
    end_year = 2040
    cut_off = 2000
    test_input = generate_test_input_df(start_year, end_year)
    processed_file = process_single_file(
        test_input["input_df"],
        "test_scenario",
        "test_iu",
        BASE_COLUMNS[0],
        BASE_COLUMNS[3],
        BASE_COLUMNS[1],
        BASE_COLUMNS[2],
        test_input["draws"],
        prevalence_marker_name=PREV_MEASURE_NAME,
        post_processing_start_time=cut_off,
        threshold=0.01,
        measure_summary_map=None
        )
    # total_length = probabilities calculated for cut off +
    # summary for all years +
    # 2 extra threshold calculations
    total_length = (end_year-cut_off) + (end_year-start_year) + 2
    assert processed_file.columns.to_list() == FINAL_COLUMNS
    assert (processed_file["iu_name"] == np.full(total_length, "test_iu")).all()
    assert (processed_file["country_code"] == np.full(total_length, "tes")).all()
    assert (processed_file["scenario"] == np.full(total_length, "test_scenario")).all()
