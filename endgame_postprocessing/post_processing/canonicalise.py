import pandas as pd

from endgame_postprocessing.post_processing import canonical_columns
from endgame_postprocessing.post_processing.custom_file_info import CustomFileInfo


def canonicalise_raw(
        raw: pd.DataFrame, file_info: CustomFileInfo, processed_prevalence_name: str
):
    raw.insert(0, canonical_columns.SCENARIO, file_info.scenario)
    raw.insert(1, canonical_columns.COUNTRY_CODE, file_info.country)
    raw.insert(2, canonical_columns.IU_NAME, file_info.iu)
    filtered_data = raw.query(f"measure == '{processed_prevalence_name}'")
    if len(filtered_data) == 0:
        raise Exception(
            f"No rows in {file_info.file_path} with measure {processed_prevalence_name}"
        )
    filtered_data.loc[:, "measure"] = canonical_columns.PROCESSED_PREVALENCE

    if canonical_columns.YEAR_ID not in raw.columns:
        raise Exception(f"Could not find {canonical_columns.YEAR_ID} column")

    if canonical_columns.MEASURE not in raw.columns:
        raise Exception(f"Could not find {canonical_columns.MEASURE} column")

    canonical_column_names = [
        canonical_columns.SCENARIO,
        canonical_columns.COUNTRY_CODE,
        canonical_columns.IU_NAME,
        canonical_columns.YEAR_ID,
        # TODO: canonical shouldn't need the age_start / age_end but these are assumed present later
        "age_start",
        "age_end",
        canonical_columns.MEASURE,
    ]
    only_canonical_columns = pd.concat(
        [filtered_data.loc[:, canonical_column_names], filtered_data.loc[:, "draw_0":]],
        axis="columns",
    )

    return only_canonical_columns.reset_index(drop=True)
