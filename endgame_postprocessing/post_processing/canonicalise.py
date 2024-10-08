import os
from pathlib import Path
import pandas as pd

from endgame_postprocessing.post_processing.dataclasses import CustomFileInfo
from endgame_postprocessing.post_processing import canoncical_columns


def canonicalise_raw(
    raw: pd.DataFrame, file_info: CustomFileInfo, processed_prevalence_name: str
):
    raw.insert(0, canoncical_columns.SCENARIO, file_info.scenario)
    raw.insert(1, canoncical_columns.COUNTRY_CODE, file_info.country)
    raw.insert(2, canoncical_columns.IU_NAME, file_info.iu)
    filtered_data = raw.query(f"measure == '{processed_prevalence_name}'")
    if len(filtered_data) == 0:
        raise Exception(
            f"No rows in {file_info.file_path} with measure {processed_prevalence_name}"
        )
    filtered_data.loc[:, "measure"] = canoncical_columns.PROCESSED_PREVALENCE

    if canoncical_columns.YEAR_ID not in raw.columns:
        raise Exception(f"Could not find {canoncical_columns.YEAR_ID} column")

    if canoncical_columns.MEASURE not in raw.columns:
        raise Exception(f"Could not find {canoncical_columns.MEASURE} column")

    return filtered_data.reset_index(drop=True)
