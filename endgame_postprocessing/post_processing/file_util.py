from .dataclasses import CustomFileInfo
from typing import Generator
import os

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
    total_scenarios = len(os.listdir(file_directory))
    for scenario_index, scenario in enumerate(os.listdir(file_directory)):
        scenario_dir_path = os.path.join(file_directory, scenario)
        if not (os.path.isdir(scenario_dir_path)):
            continue
        for country in os.listdir(scenario_dir_path):
            country_dir_path = os.path.join(file_directory, scenario, country)
            if not (os.path.isdir(country_dir_path)):
                continue
            for iu in os.listdir(country_dir_path):
                iu_dir_path = os.path.join(file_directory, scenario, country, iu)
                if not (os.path.isdir(iu_dir_path)):
                    continue
                for output_file in os.listdir(iu_dir_path):
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
