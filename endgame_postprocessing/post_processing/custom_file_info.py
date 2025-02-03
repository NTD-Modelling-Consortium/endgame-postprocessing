from dataclasses import dataclass
import re


@dataclass
class CustomFileInfo:
    """Class for the output of the custom file generator"""

    scenario_index: int
    total_scenarios: int
    scenario: str
    country: str
    iu: str
    file_path: str

    # TODO: Custom comparison logic is clunky. Needs to be better flushed out to include
    # numbers and letters (i.e 1a, 1b, etc.). Should ideally use scenario_index
    def __lt__(self, other):
        numbered_scenario_self = re.search(r'\d+', self.scenario)
        numbered_scenario_other = re.search(r'\d+', other.scenario)
        if numbered_scenario_self and numbered_scenario_other:
            if int(numbered_scenario_self.group()) == int(numbered_scenario_other.group()):
                return self.scenario < other.scenario
            return int(numbered_scenario_self.group()) < int(numbered_scenario_other.group())
        else:
            return self.scenario < other.scenario

    def __eq__(self, other):
        return self.scenario == other.scenario
