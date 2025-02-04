from typing import List, Any

import numpy as np
import pandas as pd
from numpy import ndarray, dtype

SCENARIO = "scenario"
COUNTRY_CODE = "country_code"
IU_NAME = "iu_name"
YEAR_ID = "year_id"
MEASURE = "measure"
PROCESSED_PREVALENCE = "processed_prevalence"
PROB_UNDER_THRESHOLD_MEASURE_NAME = "prob_under_threshold_measure_name"


def extract_draws(
    canonical_iu_dataframes: List[pd.DataFrame],
) -> tuple[Any, ndarray[Any, dtype[Any]]]:
    """
    Extracts draw columns from a list of canonical IU dataframes and returns the column names
    and the corresponding numerical values in a structured format. The numerical values are
    organized as a 3D NumPy array.

    Args:
        canonical_iu_dataframes (List[pd.DataFrame]): A list of dataframes where each dataframe
            contains draw columns labeled as "draw_0", "draw_1", etc., alongside other non-draw
            columns.

    Returns:
        tuple[Any, ndarray[Any, dtype[Any]]]: A tuple where the first element is the column names
            corresponding to draw columns ("draw_0", "draw_1", etc.), and the second element is
            a NumPy array with shape pxmxn (p=no.of dataframes,m,n=no.of rows and columns
            in the dataframe respectively) and containing values from these draw columns for all
            the dataframes.
    """
    columns = canonical_iu_dataframes[0].loc[:, "draw_0":].columns
    return columns, np.array(
        [iu[columns].to_numpy() for iu in canonical_iu_dataframes], dtype=float
    )
