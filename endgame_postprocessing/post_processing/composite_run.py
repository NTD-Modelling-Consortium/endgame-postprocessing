from functools import reduce
import itertools
import pandas as pd


def build_iu_case_numbers(canonical_iu_run, population) -> pd.DataFrame:
    return canonical_iu_run.loc[:, "draw_1":] * population


def read_population_data(population_data, iu):
    if iu in population_data:
        return population_data[iu]
    return 10000


def build_composite_run(canonicial_iu_runs: list[pd.DataFrame], population_data):
    # Assumptions: same number of draws in each IU run
    # Same year IDs in each one

    iu_case_numbers = [
        build_iu_case_numbers(
            canconical_iu_run,
            read_population_data(population_data, canconical_iu_run["iu_code"].iloc[0]),
        )
        for canconical_iu_run in canonicial_iu_runs
    ]

    summed_case_numbers = reduce(
        lambda left, right: left.add(right, fill_value=0), iu_case_numbers
    )

    return pd.concat(
        [canonicial_iu_runs[0][["year_id", "scenario"]], summed_case_numbers], axis=1
    )


def build_composite_run_multiple_scenarios(
    canonicial_iu_runs: list[pd.DataFrame], population_data
):
    ius_by_scenario = itertools.groupby(
        canonicial_iu_runs, lambda run: run["scenario"].iloc[0]
    )

    scenario_results = [
        build_composite_run(list(ius), population_data)
        for scenario, ius in ius_by_scenario
    ]
    return pd.concat(scenario_results, ignore_index=True)
