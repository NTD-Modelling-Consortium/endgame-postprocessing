import warnings
from collections import defaultdict

import pandas as pd

from endgame_postprocessing.post_processing import canonical_columns
from endgame_postprocessing.post_processing.canonical_results import CanonicalResults


def replicate_historic_data_in_all_scenarios(results: CanonicalResults,
                                             source_scenario: str) -> CanonicalResults:  # noqa E501
    if source_scenario not in results:
        raise ValueError(f"Invalid source_scenario: '{source_scenario}' as not in {list(results.keys())}")  # noqa E501
    updated_results = defaultdict(dict)
    ius_to_exclude = []
    for other_scenario in results:
        if other_scenario == source_scenario:
            updated_results[other_scenario] = results[other_scenario]
            continue
        updated_results[other_scenario] = {}
        for iu in results[source_scenario]:
            if iu not in results[other_scenario]:
                warnings.warn(f"IU {iu} found in {source_scenario} but not found in {other_scenario}")  # noqa E501
                ius_to_exclude.append(iu)
                continue
            _, source_scenario_data = results[source_scenario][iu]
            other_scenario_file, other_scenario_data = results[other_scenario][iu]
            first_year_of_other_scenario = other_scenario_data[canonical_columns.YEAR_ID].min()
            source_scenario_data_up_to_start = source_scenario_data.loc[
                source_scenario_data[canonical_columns.YEAR_ID] < first_year_of_other_scenario
                ]
            new_scenario_data = pd.concat([source_scenario_data_up_to_start, other_scenario_data])
            new_scenario_data["scenario"] = other_scenario_file.scenario
            updated_results[other_scenario][iu] = (other_scenario_file, new_scenario_data)

    for scenario in results:
        if scenario == source_scenario:
            continue
        ius_without_historic_data = results[scenario].keys() - results[source_scenario].keys()
        for iu in ius_without_historic_data:
            warnings.warn(
                f"IU {iu} was not found in {source_scenario} and as such will not have the historic data")  # noqa E501
            ius_to_exclude.append(iu)

    for excluded_iu in ius_to_exclude:
        for scenario in updated_results:
            if excluded_iu in updated_results[scenario]:
                del updated_results[scenario][excluded_iu]

    return updated_results
