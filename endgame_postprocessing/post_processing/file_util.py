import glob
import os
import re
import warnings
from typing import Generator

from .custom_file_info import CustomFileInfo


def custom_progress_bar_update(progress_bar, curr_index: int, total: int):
    """
    Custom update for the progress bar to work with the `post_process_file_generator` below.

    Args:
        progress_bar: the progress bar object
        curr_index (int): The current index that the progress bar should be on.
        total (int): The total number of iterations that should occur.
    """
    if progress_bar.total != total:
        progress_bar.total = total
        progress_bar.refresh()
    if progress_bar.n != curr_index & curr_index <= progress_bar.total:
        progress_bar.n = curr_index
        progress_bar.refresh()


def subdirectory_generator(directory: str):
    files_and_folders = os.listdir(directory)
    for file_or_folder in files_and_folders:
        full_path = os.path.join(directory, file_or_folder)
        if os.path.isdir(full_path):
            yield full_path, file_or_folder
        else:
            warnings.warn(f"Unexpected file {full_path} found in {directory}")


def post_process_file_generator(
        file_directory: str,
        end_of_file: str = ".csv",
) -> Generator[CustomFileInfo, None, None]:
    """
    Returns a generator for files in a given directory, only returning files that end
    with a certain string.

    Args:
        file_directory (str): The name of the file directory all the files are in. Should be in the
                                format file_directory/scenario/country/iu/output_file.csv.
        end_of_file (str): A substring that defines the files to be processed. Default is ".csv".

    Returns:
        Yields a generator, which is a tuple, of form (scenario_index, total_scenarios, scenario,
        country, iu, full_file_path).
    """
    scenario_directories = [dir for dir in subdirectory_generator(file_directory)]
    total_scenarios = len(scenario_directories)

    if total_scenarios == 0:
        raise Exception(f"No scenario directories found in {file_directory}")

    for scenario_index, (scenario_dir_path, scenario) in enumerate(
            scenario_directories
    ):
        for country_dir_path, country in subdirectory_generator(scenario_dir_path):
            for iu_dir_path, iu in subdirectory_generator(country_dir_path):
                path, directories, files = next(os.walk(iu_dir_path))
                if len(directories) != 0:
                    warnings.warn(
                        f"{len(directories)} unexpected subdirectories in IU directory {path}, "
                        "contents will be ignored"
                    )

                if len(files) == 0:
                    warnings.warn(f"No IU data files found for IU {path}")

                for output_file in files:
                    if output_file.endswith(end_of_file):
                        yield CustomFileInfo(
                            scenario_index,
                            total_scenarios,
                            scenario,
                            country,
                            iu,
                            os.path.join(
                                file_directory, scenario, country, iu, output_file
                            ),
                        )
                    else:
                        warnings.warn(
                            f"Unexpected file {output_file} in IUs directory {iu_dir_path}, "
                            f"expecting {end_of_file} only"
                        )


def get_flat_regex(file_name_regex, input_dir, glob_expression="**/*.csv"):
    files = glob.glob(glob_expression, root_dir=input_dir, recursive=True)
    for file in files:
        file_match = re.search(file_name_regex, file)
        if not file_match:
            warnings.warn(f"Unexpected file: {file}")
            continue

        groups = file_match.groupdict()
        yield CustomFileInfo(
            scenario_index=1,  # TODO - note scenarios are not ints, eg 2a
            total_scenarios=3,  # TODO
            scenario=groups.get("scenario", ""),
            country=groups.get("country", ""),
            iu=groups.get("iu_id", ""),
            file_path=f"{input_dir}/{file}",
        )


def get_matching_csv(
        path: str, historic_prefix: str, country_code: str, iu_number: str, scenario: str
):
    matching_values = glob.glob(
        os.path.join(path, f"{historic_prefix}{country_code}*{iu_number}.csv")
    )
    if len(matching_values) == 0:
        warnings.warn(
            f"IU {country_code}{iu_number} found in {scenario} but not found in histories."
        )
        return None
    if len(matching_values) > 1:
        raise Exception(
            f"Expected exactly one file for {historic_prefix}{country_code}{iu_number},"
            + f"found {len(matching_values)}"
        )
    return matching_values[0]


def list_all_historic_ius(historic_dir: str, historic_prefix: str):
    """
    List all IUs from the historic directory containing the prefix and maps the IDs to file paths.

    Args:
        historic_dir (str): Directory where historic IUs are stored.
        historic_prefix (str): Prefix used to identify the files.

    Returns:
        dict: A mapping of IU identifiers to file paths.
    """

    iu_file_map = {}
    if historic_dir is None:
        return iu_file_map

    files = glob.glob(
        os.path.join(historic_dir, f"{historic_prefix}*.csv"), recursive=True
    )

    regex_pattern = r"(?P<country>[A-Z]{3}).{0,5}(?P<iu_id>[\d]{5}).csv"

    for file in files:
        file_match = re.search(regex_pattern, file)
        if file_match:
            iu_string = f"{file_match.group('country')}{file_match.group('iu_id')}"
            iu_file_map[iu_string] = file  # Map the IU string to the file path
    return iu_file_map
