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
    filtered_data.loc[:, "measure"] = canoncical_columns.PROCESSED_PREVALENCE

    if canoncical_columns.YEAR_ID not in raw.columns:
        raise Exception(f"Could not find {canoncical_columns.YEAR_ID} column")

    if canoncical_columns.MEASURE not in raw.columns:
        raise Exception(f"Could not find {canoncical_columns.MEASURE} column")

    return filtered_data.reset_index(drop=True)


def write_canonical(
    root_dir, file_info: CustomFileInfo, canonical_result: pd.DataFrame
):
    scenario = file_info.scenario
    country = file_info.country
    iu = file_info.iu
    file_name = f"canonical_{iu}.csv"
    path = Path(f"{root_dir}/canonical_results/{scenario}/{country}/")
    path.mkdir(parents=True, exist_ok=True)
    canonical_result.to_csv(f"{path}/{file_name}")
