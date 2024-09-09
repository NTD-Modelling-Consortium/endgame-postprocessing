import numpy as np
import pandas as pd


def first_row(df):
    return df.iloc[0]


def recombine_ages(raw_data: pd.DataFrame):
    each_intensity_measure = [
        df for _, df in raw_data.groupby(["Time", "intensity", "measure"])
    ]
    aggregate_functions = {c: first_row for c in raw_data.columns}
    aggregate_functions["draw_1"] = np.sum
    aggregate_functions["age_start"] = np.min
    aggregate_functions["age_end"] = np.max

    aggregated_by_group = [
        measure_values.aggregate(aggregate_functions)
        for measure_values in each_intensity_measure
    ]

    return pd.concat(aggregated_by_group, axis=1).T
