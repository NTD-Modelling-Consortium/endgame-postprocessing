from functools import reduce
import itertools
import pandas as pd
from endgame_postprocessing.post_processing import canoncical_columns


def build_iu_case_numbers(canonical_iu_run, population) -> pd.DataFrame:
    return canonical_iu_run.loc[:, "draw_0":] * population

def get_ius_per_country(population_data, country_code, filter_measure):
    if country_code in population_data["country_code"].unique():
        return (population_data
            .iloc[(population_data["country_code"] == country_code)]
            .query(f"{filter_measure} == 'True'")["iu_name"]
            .nunique()
        )
    return 100

def read_population_data(population_data, iu):
    if iu in population_data:
        return population_data[iu]
    return 10000


def build_composite_run(canonicial_iu_runs: list[pd.DataFrame], population_data, is_africa=False):
    # Assumptions: same number of draws in each IU run
    # Same year IDs in each one

    iu_case_numbers = [
        build_iu_case_numbers(
            canconical_iu_run,
            read_population_data(
                population_data, canconical_iu_run[canoncical_columns.IU_NAME].iloc[0]
            ),
        )
        for canconical_iu_run in canonicial_iu_runs
    ]

    summed_case_numbers = reduce(
        lambda left, right: left.add(right, fill_value=0), iu_case_numbers
    )

    total_population = sum(
        [
            read_population_data(
                population_data, canconical_iu_run[canoncical_columns.IU_NAME].iloc[0]
            )
            for canconical_iu_run in canonicial_iu_runs
        ]
    )
    prevalence = summed_case_numbers / total_population
    columns_to_use = [
        canoncical_columns.YEAR_ID,
        canoncical_columns.SCENARIO,
        canoncical_columns.COUNTRY_CODE,
        canoncical_columns.MEASURE,
    ]
    if is_africa:
        columns_to_use.remove(canoncical_columns.COUNTRY_CODE)
    return pd.concat(
        [
            canonicial_iu_runs[0][
                columns_to_use
            ],
            prevalence,
        ],
        axis=1,
    )


def build_composite_run_multiple_scenarios(
    canonicial_iu_runs: list[pd.DataFrame], population_data, is_africa=False
):
    ius_by_scenario = itertools.groupby(
        canonicial_iu_runs, lambda run: run["scenario"].iloc[0]
    )

    scenario_results = [
        build_composite_run(list(ius), population_data, is_africa)
        for _, ius in ius_by_scenario
    ]
    return pd.concat(scenario_results, ignore_index=True)
