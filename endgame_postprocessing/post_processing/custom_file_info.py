from dataclasses import dataclass


@dataclass
class CustomFileInfo:
    """Class for the output of the custom file generator"""

    scenario_index: int
    total_scenarios: int
    scenario: str
    country: str
    iu: str
    file_path: str

    def __lt__(self, other):
        return self.file_path < other.file_path

    def __eq__(self, other):
        return self.file_path == other.file_path
