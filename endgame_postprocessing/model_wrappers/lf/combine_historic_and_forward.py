import pandas as pd
from endgame_postprocessing.post_processing import (
    file_util,
    output_directory_structure,
)


class MissingHistoricDataException(Exception):
    def __init__(self, iu):
        super().__init__(f"Missing IU: {iu} in historic data")


class MismatchedColumnsException(Exception):
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
            r"(?P<iu_id>(?P<country>[A-Z]{3})\d{5})_(?P<scenario>scenario_\d+)_canonical.csv",
            historic_canonical_data_path,
        )
    }

    forward_data_file_infos = file_util.get_flat_regex(
        r"(?P<iu_id>(?P<country>[A-Z]{3})\d{5})_(?P<scenario>scenario_\d+)_canonical.csv",
        forward_canonical_data_path,
    )
    for forward_file in forward_data_file_infos:
        if forward_file.iu not in historic_data_file_infos:
            raise MissingHistoricDataException(forward_file.iu)
        historic_file = historic_data_file_infos[forward_file.iu]
        historic_data = pd.read_csv(historic_file.file_path)
        forward_data = pd.read_csv(forward_file.file_path)

        if not _all_columns_match(historic_data, forward_data):
            raise (MismatchedColumnsException(forward_file.iu))

        all_data = pd.concat([historic_data, forward_data])
        all_data["scenario"] = forward_file.scenario
        output_directory_structure.write_canonical(output_path, forward_file, all_data)
