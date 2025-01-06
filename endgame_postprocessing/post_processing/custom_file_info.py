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
