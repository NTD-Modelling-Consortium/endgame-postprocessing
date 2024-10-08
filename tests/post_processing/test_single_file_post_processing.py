import pytest
import numpy as np
import numpy.testing as npt
from endgame_postprocessing.post_processing.constants import FINAL_COLUMNS
from endgame_postprocessing.post_processing import single_file_post_processing, measures
from tests.test_helper_functions import (
    PREV_MEASURE_NAME,
    generate_test_input,
    generate_test_input_df,
    check_if_columns_is_float,
)


def test_validate_measure_map_success_blank():
    map_to_use = None
    validated_map = single_file_post_processing.validate_measure_map(
        map_to_use, PREV_MEASURE_NAME
    )
    assert validated_map == {PREV_MEASURE_NAME: measures.measure_summary_float}


def test_validate_measure_map_success_none():
    map_to_use = {"random_value": None}
    validated_map = single_file_post_processing.validate_measure_map(
        map_to_use, PREV_MEASURE_NAME
    )
    assert validated_map == {
        "random_value": measures.measure_summary_float,
        PREV_MEASURE_NAME: measures.measure_summary_float,
    }


def test_validate_measure_map_success_custom_func():
    def temp_func():
        return "Yay it works"

    map_to_use = {PREV_MEASURE_NAME: temp_func}
    validated_map = single_file_post_processing.validate_measure_map(
        map_to_use, PREV_MEASURE_NAME
    )
    assert validated_map == {PREV_MEASURE_NAME: temp_func}


def test_validate_measure_map_success_empty_dict():
    map_to_use = {}
    validated_map = single_file_post_processing.validate_measure_map(
        map_to_use, PREV_MEASURE_NAME
    )
    assert validated_map == {PREV_MEASURE_NAME: measures.measure_summary_float}


def test_validate_measure_map_fail_uncallable():
    map_to_use = {"random_value": "fake_function"}
    with pytest.raises(ValueError):
        single_file_post_processing.validate_measure_map(map_to_use, PREV_MEASURE_NAME)


def test_filter_out_old_data_success():
    start_time = 1970
    end_time = 2040
    cut_off = 2000
    test_input = generate_test_input(start_time, end_time)
    filtered_data = single_file_post_processing._filter_out_old_data(
        test_input["input"],
        test_input["year_loc"],
        post_processing_start_time=cut_off,
        post_processing_end_time=2050,
    )
    assert filtered_data.shape[0] == end_time - cut_off
    assert np.amin(filtered_data[:, 0].astype(float)) >= cut_off


def test_filter_out_old_data_success_all_data_is_before():
    start_time = 1970
    end_time = 2040
    cut_off = 2050
    test_input = generate_test_input(start_time, end_time)
    filtered_data = single_file_post_processing._filter_out_old_data(
        test_input["input"],
        test_input["year_loc"],
        post_processing_start_time=cut_off,
        post_processing_end_time=2050,
    )
    assert filtered_data.shape[0] == 0


def test_filter_out_old_data_success_all_data_is_after():
    start_time = 1970
    end_time = 2040
    cut_off = 1900
    test_input = generate_test_input(start_time, end_time)
    filtered_data = single_file_post_processing._filter_out_old_data(
        test_input["input"],
        test_input["year_loc"],
        post_processing_start_time=cut_off,
        post_processing_end_time=2050,
    )
    assert (test_input["input"] == filtered_data).all()


def test_filter_out_old_data_fail_cutoff_not_numeric():
    start_time = 1970
    end_time = 2040
    cut_off = "the year two-thousand"
    test_input = generate_test_input(start_time, end_time)
    with pytest.raises(Exception):
        single_file_post_processing._filter_out_old_data(
            test_input["input"], test_input["year_loc"], cut_off
        )


def test_calculate_probabilities_and_thresholds_success():
    test_input = generate_test_input()
    prob_and_threshold_data = (
        single_file_post_processing._calculate_probabilities_and_thresholds(
            filtered_model_outputs=test_input["input"],
            year_column_loc=test_input["year_loc"],
            measure_column_loc=test_input["measure_loc"],
            age_start_column_loc=test_input["age_start_loc"],
            age_end_column_loc=test_input["age_end_loc"],
            draws_loc=test_input["draws_loc"],
            prevalence_marker_name=PREV_MEASURE_NAME,
            threshold=0.1,
            pct_runs_under_threshold=[0.8, 0.9],
        )
    )
    npt.assert_array_equal(
        prob_and_threshold_data[:, test_input["measure_loc"]],
        np.concatenate(
            (
                np.full(test_input["total_rows"], "prob_under_threshold_prevalence"),
                [
                    "year_of_80pct_runs_under_threshold",
                    "year_of_90pct_runs_under_threshold",
                ],
            )
        ),
    )
    assert prob_and_threshold_data.shape[0] == test_input["input"].shape[0] + 2
    assert prob_and_threshold_data.shape[1] == 16
    assert check_if_columns_is_float(
        prob_and_threshold_data, [test_input["measure_loc"]]
    )


# is this really a success case? Should we error out if the year_id column
# cannot be coerced to numeric?
def test_calculate_probabilities_and_thresholds_success_non_numeric_year():
    test_input = generate_test_input()
    prob_and_threshold_data = (
        single_file_post_processing._calculate_probabilities_and_thresholds(
            filtered_model_outputs=test_input["input"],
            year_column_loc=test_input["measure_loc"],
            measure_column_loc=test_input["measure_loc"],
            age_start_column_loc=test_input["age_start_loc"],
            age_end_column_loc=test_input["age_end_loc"],
            draws_loc=test_input["draws_loc"],
            prevalence_marker_name=PREV_MEASURE_NAME,
            threshold=0.1,
            pct_runs_under_threshold=[0.9],
        )
    )
    with pytest.raises(AssertionError):
        check_if_columns_is_float(prob_and_threshold_data, [test_input["measure_loc"]])


def test_calculate_probabilities_and_thresholds_fail_no_year_column_exists():
    test_input = generate_test_input()
    with pytest.raises(IndexError):
        single_file_post_processing._calculate_probabilities_and_thresholds(
            filtered_model_outputs=test_input["input"],
            year_column_loc=30,
            measure_column_loc=test_input["measure_loc"],
            age_start_column_loc=test_input["age_start_loc"],
            age_end_column_loc=test_input["age_end_loc"],
            draws_loc=test_input["draws_loc"],
            prevalence_marker_name=PREV_MEASURE_NAME,
            threshold=0.1,
            pct_runs_under_threshold=[0.9],
        )


def test_calculate_probabilities_and_thresholds_fail_invalid_input_string():
    num_draws = 10
    test_input = generate_test_input(num_draws=num_draws)
    test_input["input"][:, test_input["draws_loc"]] = np.full(
        (test_input["total_rows"], num_draws), "a"
    )
    with pytest.raises(ValueError):
        single_file_post_processing._calculate_probabilities_and_thresholds(
            filtered_model_outputs=test_input["input"],
            year_column_loc=test_input["year_loc"],
            measure_column_loc=test_input["measure_loc"],
            age_start_column_loc=test_input["age_start_loc"],
            age_end_column_loc=test_input["age_end_loc"],
            draws_loc=test_input["draws_loc"],
            prevalence_marker_name=PREV_MEASURE_NAME,
            threshold=0.1,
            pct_runs_under_threshold=[0.9],
        )


def test_summarize_measures_success():
    test_input = generate_test_input()
    summary_data = single_file_post_processing._summarize_measures(
        raw_output_data=test_input["input"],
        year_column_loc=test_input["year_loc"],
        measure_column_loc=test_input["measure_loc"],
        age_start_column_loc=test_input["age_start_loc"],
        age_end_column_loc=test_input["age_end_loc"],
        draws_loc=test_input["draws_loc"],
        prevalence_marker_name=PREV_MEASURE_NAME,
        measure_summary_map={PREV_MEASURE_NAME: measures.measure_summary_float},
    )
    assert summary_data.shape[0] == test_input["input"].shape[0]
    assert summary_data.shape[1] == 16
    assert np.isin(PREV_MEASURE_NAME, summary_data[:, test_input["measure_loc"]])
    assert check_if_columns_is_float(summary_data, [test_input["measure_loc"]])

def test_summarize_measures_multiple_measures_success():
    test_input1 = generate_test_input()
    test_input2 = generate_test_input()
    test_input2["input"][:, 3] = "measure_2"
    test_input = np.row_stack((test_input1["input"], test_input2["input"]))
    summary_data = single_file_post_processing._summarize_measures(
        raw_output_data=test_input,
        year_column_loc=test_input1["year_loc"],
        measure_column_loc=test_input1["measure_loc"],
        age_start_column_loc=test_input1["age_start_loc"],
        age_end_column_loc=test_input1["age_end_loc"],
        draws_loc=test_input1["draws_loc"],
        prevalence_marker_name=PREV_MEASURE_NAME,
        measure_summary_map={
            PREV_MEASURE_NAME: measures.measure_summary_float,
            "measure_2": measures.measure_summary_float,
        },
    )
    assert summary_data.shape[0] == test_input.shape[0]
    assert summary_data.shape[1] == 16
    assert (
        np.concatenate((
            np.full(test_input1["total_rows"], PREV_MEASURE_NAME),
            np.full(test_input2["total_rows"], "measure_2")
        )) ==
        summary_data[:, test_input1["measure_loc"]]
    ).all()
    assert check_if_columns_is_float(summary_data, [test_input1["measure_loc"]])


def test_summarize_measures_fail_invalid_input_string():
    num_draws = 10
    test_input = generate_test_input(num_draws=num_draws)
    test_input["input"][:, test_input["draws_loc"]] = np.full(
        (test_input["total_rows"], num_draws), "a"
    )
    with pytest.raises(ValueError):
        single_file_post_processing._summarize_measures(
            raw_output_data=test_input["input"],
            year_column_loc=test_input["year_loc"],
            measure_column_loc=test_input["measure_loc"],
            age_start_column_loc=test_input["age_start_loc"],
            age_end_column_loc=test_input["age_end_loc"],
            draws_loc=test_input["draws_loc"],
            prevalence_marker_name=PREV_MEASURE_NAME,
            measure_summary_map={PREV_MEASURE_NAME: measures.measure_summary_float},
        )


def test_summarize_measures_returns_empty_df_wrong_prev_measure_name():
    test_input = generate_test_input()
    summary_data = single_file_post_processing._summarize_measures(
        raw_output_data=test_input["input"],
        year_column_loc=test_input["year_loc"],
        measure_column_loc=test_input["measure_loc"],
        age_start_column_loc=test_input["age_start_loc"],
        age_end_column_loc=test_input["age_end_loc"],
        draws_loc=test_input["draws_loc"],
        prevalence_marker_name="wrong_measure",
        measure_summary_map={"wrong_measure": measures.measure_summary_float},
    )
    assert summary_data.shape == (0, 16)


def test_summarize_measures_returns_empty_df_wrong_measure_loc():
    test_input = generate_test_input()
    summary_data = single_file_post_processing._summarize_measures(
        raw_output_data=test_input["input"],
        year_column_loc=test_input["year_loc"],
        measure_column_loc=test_input["year_loc"],
        age_start_column_loc=test_input["age_start_loc"],
        age_end_column_loc=test_input["age_end_loc"],
        draws_loc=test_input["draws_loc"],
        prevalence_marker_name=PREV_MEASURE_NAME,
        measure_summary_map={PREV_MEASURE_NAME: measures.measure_summary_float},
    )
    assert summary_data.shape == (0, 16)


def test_summarize_measures_fail_bad_custom_summary_func():
    def tmp_func():
        return "My goal is to break the function"

    test_input = generate_test_input()
    with pytest.raises(TypeError):
        single_file_post_processing._summarize_measures(
            raw_output_data=test_input["input"],
            year_column_loc=test_input["year_loc"],
            measure_column_loc=test_input["year_loc"],
            age_start_column_loc=test_input["age_start_loc"],
            age_end_column_loc=test_input["age_end_loc"],
            draws_loc=test_input["draws_loc"],
            prevalence_marker_name=PREV_MEASURE_NAME,
            measure_summary_map={PREV_MEASURE_NAME: tmp_func},
        )


def test_process_single_file_success():
    start_year = 1970
    end_year = 2040
    cut_off = 2000
    num_draws = 10
    test_input = generate_test_input_df(start_year, end_year, num_draws)
    processed_file = single_file_post_processing.process_single_file(
        raw_model_outputs=test_input["input_df"],
        scenario="test_scenario",
        iuName="test_iu",
        num_draws=num_draws,
        prevalence_marker_name=PREV_MEASURE_NAME,
        post_processing_start_time=cut_off,
        threshold=0.01,
        measure_summary_map=None,
    )
    # total_length = probabilities calculated for cut off +
    # summary for all years +
    # 2 extra threshold calculations
    total_length = (end_year - cut_off) + (end_year - start_year) + 1
    assert processed_file.columns.to_list() == FINAL_COLUMNS
    assert (processed_file["iu_name"] == np.full(total_length, "test_iu")).all()
    assert (processed_file["country_code"] == np.full(total_length, "tes")).all()
    assert (processed_file["scenario"] == np.full(total_length, "test_scenario")).all()


def test_process_single_file_fail_column_name_doesnt_exist():
    def failed_key_helper(input_df, column_names, num_draws):
        input_df.columns = column_names
        with pytest.raises(KeyError):
            single_file_post_processing.process_single_file(
                raw_model_outputs=input_df,
                scenario="test_scenario",
                iuName="test_iu",
                num_draws=num_draws,
                prevalence_marker_name=PREV_MEASURE_NAME,
                post_processing_start_time=cut_off,
                threshold=0.01,
                measure_summary_map=None,
            )

    start_year = 1970
    end_year = 2040
    cut_off = 2000
    num_draws = 10
    test_input = generate_test_input_df(start_year, end_year, num_draws)
    test_columns = ["year", "age_start", "age_end", "measure"] + [
        "draw_" + str(i) for i in range(num_draws)
    ]
    failed_key_helper(test_input["input_df"], test_columns, num_draws)
    test_columns = ["year_id", "age_st", "age_end", "measure"] + [
        "draw_" + str(i) for i in range(num_draws)
    ]
    failed_key_helper(test_input["input_df"], test_columns, num_draws)
    test_columns = ["year_id", "age_start", "age", "measure"] + [
        "draw_" + str(i) for i in range(num_draws)
    ]
    failed_key_helper(test_input["input_df"], test_columns, num_draws)
    test_columns = ["year_id", "age_start", "age_end", "measures"] + [
        "draw_" + str(i) for i in range(num_draws)
    ]
    failed_key_helper(test_input["input_df"], test_columns, num_draws)
    test_columns = ["year_id", "age_start", "age_end", "measures"] + [
        "draw_" + str(i) for i in range(num_draws)
    ]
    failed_key_helper(test_input["input_df"], test_columns, 11)
    test_columns = ["year_id", "age_start", "age_end", "measures"] + [
        "draws_" + str(i) for i in range(num_draws)
    ]
    failed_key_helper(test_input["input_df"], test_columns, num_draws)
