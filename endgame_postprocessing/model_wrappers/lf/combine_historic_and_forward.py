import dataclasses
import pandas as pd
from endgame_postprocessing.post_processing import (
    canoncical_columns,
    file_util,
    output_directory_structure,
)


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
        historic_file = historic_data_file_infos[forward_file.iu]
        historic_data = pd.read_csv(historic_file.file_path)
        forward_data = pd.read_csv(forward_file.file_path)
        # scenario = forward_data[canoncical_columns.SCENARIO].iat[0]
        all_data = pd.concat([historic_data, forward_data])
        # new_file = dataclasses.replace(forward_file, scenario=scenario)
        historic_data["scenario"] = forward_file.scenario
        output_directory_structure.write_canonical(output_path, forward_file, all_data)


if __name__ == "__main__":
    historic_canoncial = "local_data/lf-historic-output/canonical_results"
    forward_canoncical = "local_data/lf-output/canonical_results"
    combined_output = "local_data/lf-combined-output"
    combine_historic_and_forward(
        historic_canoncial, forward_canoncical, combined_output
    )
