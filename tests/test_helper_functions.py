import numpy as np
import pandas as pd

PERCENTILES_TO_TEST = [2.5, 5, 10]
EXPECTED_ROWS = 5
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
        "draws": ["draw_" + str(i) for i in range(num_draws)],
        "input_df": pd.DataFrame(
            matrix_input,
            columns = BASE_COLUMNS +
            ["draw_" + str(i) for i in range(num_draws)]
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