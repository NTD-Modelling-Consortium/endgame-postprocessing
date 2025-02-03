import warnings

import pandas as pd

from endgame_postprocessing.post_processing import (
    canonical_file_name,
    file_util,
    output_directory_structure, canonical_columns,
)


class MissingHistoricDataException(Warning):
    def __init__(self, iu):
        super().__init__(f"Missing IU: {iu} in historic data")


class MismatchedColumnsException(Warning):
    def __init__(self, iu):
        super().__init__(f"{iu} different columns in historic and forward projection")


def _all_columns_match(df1: pd.DataFrame, df2: pd.DataFrame):
    matching_columns = df1.columns.intersection(df2.columns)
    return len(matching_columns) == len(df1.columns)


def combine_historic_and_forward(
        historic_canonical_data_path, forward_canonical_data_path, output_path
):
    historic_data_file_infos = {
        file_info.iu: file_info
        for file_info in file_util.get_flat_regex(
            canonical_file_name.get_regex(),
            historic_canonical_data_path,
        )
    }

    forward_data_file_infos = file_util.get_flat_regex(
        canonical_file_name.get_regex(),
        forward_canonical_data_path,
    )
    for forward_file in forward_data_file_infos:
        if forward_file.iu not in historic_data_file_infos:
            warnings.warn(MissingHistoricDataException(forward_file.iu))
            continue
        historic_file = historic_data_file_infos[forward_file.iu]
        historic_data = pd.read_csv(historic_file.file_path)
        forward_data = pd.read_csv(forward_file.file_path)

        if not _all_columns_match(historic_data, forward_data):
            warnings.warn(MismatchedColumnsException(forward_file.iu))
            continue

        first_year_of_forward_data = forward_data[canonical_columns.YEAR_ID].min()
        historic_data_up_to_start = historic_data.loc[
            historic_data[canonical_columns.YEAR_ID] < first_year_of_forward_data
            ]

        all_data = pd.concat([historic_data_up_to_start, forward_data])
        all_data["scenario"] = forward_file.scenario
        output_directory_structure.write_canonical(output_path, forward_file, all_data)
