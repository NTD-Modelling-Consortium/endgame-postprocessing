from dataclasses import dataclass
import glob
from pathlib import Path
import re
import shutil


def _raw_sth_file_name(iu, worm, scenario):
    return f"ntdmc-{iu}-{worm}-group_001-{scenario}-group_001-200_simulations.csv"


def _raw_sch_file_name(iu, worm, scenario):
    return f"ntdmc-{iu}-{worm}-group_001-{scenario}-survey_type_kk2-group_001-200_simulations.csv"


@dataclass
class Configuration:
    worm_remapping: dict[str, str]
    output_file_name: callable[str, str, str, str]


STH_Config = Configuration(
    worm_remapping={
        "Asc": "ascaris",
        "Hook": "hookworm",
        "Tri": "trichuris",
    },
    output_file_name=_raw_sth_file_name,
)

SCH_Config = Configuration(
    worm_remapping={
        "Haema": "haematobium",
        "Man_Low": "mansoni_low_burden",
        "Man_High": "mansoni_high_burden",
    },
    output_file_name=_raw_sch_file_name,
)


def _standard_worm_from_historic(historic_worm_name, config: Configuration):
    if historic_worm_name not in config.worm_remapping:
        raise Exception(f"Unexpected worm: {historic_worm_name}")
    return config.worm_remapping[historic_worm_name]


def _get_standard_name_for_historic_file(historic_file_name, config: Configuration):
    old_file_name_regex = r"PrevDataset_(?P<worm>\w+)_(?P<iu_id>(?P<country>[A-Z]{3})\d{5})(?P<scenario>).csv"
    file_match = re.search(old_file_name_regex, historic_file_name)
    if not file_match:
        return None
    scenario="scenario_0"
    iu=file_match.group("iu_id")
    historic_worm_name = file_match.group("worm")
    standard_worm_name = _standard_worm_from_historic(historic_worm_name, config)
    new_file_name = config.output_file_name(iu, standard_worm_name, scenario)
    return new_file_name


def get_standard_name_for_historic_sth_file(historic_file_name):
    return _get_standard_name_for_historic_file(historic_file_name, STH_Config)


def get_standard_name_for_historic_sch_file(historic_file_name):
    return _get_standard_name_for_historic_file(historic_file_name, SCH_Config)


def rename_historic_sth_files(historic_input_dir, historic_renamed_raw_dir):
    return rename_historic_files(
        historic_input_dir,
        historic_renamed_raw_dir,
        get_standard_name_for_historic_sth_file,
    )


def rename_historic_sch_files(historic_input_dir, historic_renamed_raw_dir):
    return rename_historic_files(
        historic_input_dir,
        historic_renamed_raw_dir,
        get_standard_name_for_historic_sch_file,
    )


def rename_historic_files(
    historic_input_dir, historic_renamed_raw_dir, rename_function
):
    Path(historic_renamed_raw_dir).mkdir(parents=True)
    files = glob.glob(
        "**/*.csv", root_dir=historic_input_dir, recursive=True
    )

    for file_name in files:
        new_file_name = rename_function(file_name)
        if new_file_name is None:
            continue
        old_file_path = f"{historic_input_dir}/{file_name}"
        new_file_path = f"{historic_renamed_raw_dir}/{new_file_name}"
        print(new_file_path)
        shutil.copy(old_file_path, new_file_path)
