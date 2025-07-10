import itertools
from typing import List

import numpy as np
import pandas as pd
import pandas.testing as pdt

from endgame_postprocessing.post_processing import composite_run, canonical_columns
from endgame_postprocessing.post_processing.disease import Disease
from endgame_postprocessing.post_processing.iu_data import IUData, IUSelectionCriteria
from tests.test_util.create_dummy_pop_file import create_dummy_population_file_for_disease_with_years, \
    create_dummy_population_file_for_disease


def test_build_composite_run_from_one_iu():
    canoncial_iu = pd.DataFrame(
        {
            "iu_name"     : ["AAA00001"] * 2,
            "scenario"    : ["scenario_1"] * 2,
            "country_code": ["AAA"] * 2,
            "measure"     : ["processed_prevalence"] * 2,
            "year_id"     : [2010, 2011],
            "draw_0"      : [0.2, 0.3],
            "draw_1"      : [0.3, 0.4],
        }
    )
    population_data = IUData(
        create_dummy_population_file_for_disease(Disease.LF,
                                                 {"AAA00001": 100}),
        Disease.LF,
        iu_selection_criteria=IUSelectionCriteria.ALL_IUS,
    )

    prevalences_df = _compute_prevalences_in_country([canoncial_iu], population_data)

    metadata_df = pd.DataFrame(
        {
            "year_id"     : [2010, 2011],
            "scenario"    : ["scenario_1"] * 2,
            "country_code": ["AAA"] * 2,
            "measure"     : ["processed_prevalence"] * 2,
        }
    )
    expected = metadata_df.copy()
    expected[prevalences_df.columns] = prevalences_df.values

    result = composite_run.build_composite_run([canoncial_iu], population_data)
    pdt.assert_frame_equal(
        result,
        expected,
    )


def test_build_composite_run_from_two_iu_but_second_iu_ignored():
    canoncial_iu1 = pd.DataFrame(
        {
            "year_id"     : [2010, 2011],
            "scenario"    : ["scenario_1"] * 2,
            "country_code": ["AAA"] * 2,
            "measure"     : ["processed_prevalence"] * 2,
            "iu_name"     : ["AAA00001"] * 2,
            "draw_0"      : [0.2, 0.3],
            "draw_1"      : [0.3, 0.4],
        }
    )
    canoncial_iu2 = pd.DataFrame(
        {
            "year_id"     : [2010, 2011],
            "scenario"    : ["scenario_1"] * 2,
            "country_code": ["AAA"] * 2,
            "measure"     : ["processed_prevalence"] * 2,
            "iu_name"     : ["AAA00002"] * 2,
            "draw_0"      : [0.8, 0.9],
            "draw_1"      : [0.8, 0.9],
        }
    )
    canonical_ius = [canoncial_iu1, canoncial_iu2]
    population_data = IUData(
        create_dummy_population_file_for_disease(Disease.LF,
                                                 {
                                                     "AAA00001": 100,
                                                     "AAA00002": 0,
                                                 }),
        Disease.LF,
        iu_selection_criteria=IUSelectionCriteria.ALL_IUS,
    )

    metadata_df = pd.DataFrame(
        {
            "year_id"     : [2010, 2011],
            "scenario"    : ["scenario_1"] * 2,
            "country_code": ["AAA"] * 2,
            "measure"     : ["processed_prevalence"] * 2,
        }
    )

    prevalences_df = _compute_prevalences_in_country(canonical_ius, population_data)
    expected = metadata_df.copy()
    expected[prevalences_df.columns] = prevalences_df.values

    result = composite_run.build_composite_run(
        canonical_ius, population_data
    )

    pdt.assert_frame_equal(
        result,
        expected,
    )


def test_build_composite_run_from_two_equal_sized_ius():
    canoncial_iu1 = pd.DataFrame(
        {
            "year_id"     : [2010],
            "scenario"    : ["scenario_1"],
            "country_code": ["AAA"],
            "measure"     : ["processed_prevalence"],
            "iu_name"     : ["AAA00001"],
            "draw_0"      : [0.2],
            "draw_1"      : [0.3],
        }
    )
    canoncial_iu2 = pd.DataFrame(
        {
            "year_id"     : [2010],
            "scenario"    : ["scenario_1"],
            "country_code": ["AAA"],
            "measure"     : ["processed_prevalence"],
            "iu_name"     : ["AAA00002"],
            "draw_0"      : [0.8],
            "draw_1"      : [0.9],
        }
    )
    canonical_ius = [canoncial_iu1, canoncial_iu2]
    population_data = IUData(
        create_dummy_population_file_for_disease(Disease.LF,
                                                 {
                                                     "AAA00001": 10,
                                                     "AAA00002": 10,
                                                 }),
        disease=Disease.LF,
        iu_selection_criteria=IUSelectionCriteria.ALL_IUS,
    )

    prevalences_df = _compute_prevalences_in_country(canonical_ius, population_data)
    metadata_df = pd.DataFrame(
        {
            "year_id"     : [2010],
            "scenario"    : ["scenario_1"],
            "country_code": ["AAA"],
            "measure"     : ["processed_prevalence"],
        }
    )
    expected = metadata_df.copy()
    expected[prevalences_df.columns] = prevalences_df.values

    result = composite_run.build_composite_run(canonical_ius, population_data)
    pdt.assert_frame_equal(
        result,
        expected
    )


def test_build_composite_run_retains_year_id():
    canoncial_iu1 = pd.DataFrame(
        {
            "iu_name"     : ["AAA00001"] * 2,
            "scenario"    : ["scenario_1"] * 2,
            "country_code": ["AAA"] * 2,
            "measure"     : ["processed_prevalence"] * 2,
            "year_id"     : [2010, 2011],
            "draw_0"      : [0.2] * 2,
            "draw_1"      : [0.3] * 2,
        }
    )
    canoncial_iu2 = pd.DataFrame(
        {
            "iu_name"     : ["AAA00002"] * 2,
            "scenario"    : ["scenario_1"] * 2,
            "country_code": ["AAA"] * 2,
            "measure"     : ["processed_prevalence"] * 2,
            "year_id"     : [2010, 2011],
            "draw_0"      : [0.8] * 2,
            "draw_1"      : [0.9] * 2,
        }
    )

    canonical_ius = [canoncial_iu1, canoncial_iu2]
    iu_metadata = IUData(
        input_data=create_dummy_population_file_for_disease_with_years(
            Disease.LF,
            iu_yearly_population_map={
                "AAA00001": {
                    2010: 10,
                    2011: 15,
                },
                "AAA00002": {
                    2010: 20,
                    2011: 25,
                }
            }
        ),
        disease=Disease.LF,
        iu_selection_criteria=IUSelectionCriteria.ALL_IUS
    )

    prevalences_df = _compute_prevalences_in_country(canonical_ius, iu_metadata)
    metadata_df = pd.DataFrame(
        {
            "year_id"     : [2010, 2011],
            "scenario"    : ["scenario_1"] * 2,
            "country_code": ["AAA"] * 2,
            "measure"     : ["processed_prevalence"] * 2,
        }
    )
    expected = metadata_df.copy()
    expected[prevalences_df.columns] = prevalences_df.values

    result = composite_run.build_composite_run(
        canonical_ius,
        iu_metadata,
    )

    pdt.assert_frame_equal(
        result,
        expected,
    )


def test_build_composite_multiple_scenarios():
    canoncial_iu_scenario_1 = pd.DataFrame(
        {
            "iu_name"     : ["AAA00001"] * 2,
            "scenario"    : ["scenario_1"] * 2,
            "country_code": ["AAA"] * 2,
            "measure"     : ["processed_prevalence"] * 2,
            "year_id"     : [2010, 2011],
            "draw_0"      : [0.2] * 2,
            "draw_1"      : [0.3] * 2,
        }
    )
    canoncial_iu_scenario_2 = pd.DataFrame(
        {
            "iu_name"     : ["AAA00001"] * 2,
            "scenario"    : ["scenario_2"] * 2,
            "country_code": ["AAA"] * 2,
            "measure"     : ["processed_prevalence"] * 2,
            "year_id"     : [2010, 2011],
            "draw_0"      : [0.8] * 2,
            "draw_1"      : [0.9] * 2,
        }
    )
    canonical_ius = [canoncial_iu_scenario_1, canoncial_iu_scenario_2]
    population_data = IUData(
        pd.DataFrame(
            {
                "IU_CODE"               : ["AAA00001"],
                "ADMIN0ISO3"            : ["AAA"],
                "Priority_Population_LF": [10],
            }
        ),
        disease=Disease.LF,
        iu_selection_criteria=IUSelectionCriteria.ALL_IUS,
    )
    metadata_df = pd.DataFrame(
        {
            "year_id"     : [2010, 2011, 2010, 2011],
            "scenario"    : ["scenario_1", "scenario_1", "scenario_2", "scenario_2"],
            "country_code": ["AAA"] * 4,
            "measure"     : ["processed_prevalence"] * 4,
        }
    )
    prevalences = _compute_prevalences_in_country(canonical_ius, population_data)

    # Merge using the metadata DataFrame's index to preserve row order
    expected = metadata_df.copy()
    expected[prevalences.columns] = prevalences.values

    result = composite_run.build_composite_run_multiple_scenarios(
        canonical_ius,
        population_data)
    pdt.assert_frame_equal(
        result,
        expected
    )


def test_build_composite_multiple_scenarios_with_longitudinal_population_data():
    canonical_iu_scenario_1 = pd.DataFrame(
        {
            "iu_name"     : ["AAA00001"] * 2,
            "scenario"    : ["scenario_1"] * 2,
            "country_code": ["AAA"] * 2,
            "measure"     : ["processed_prevalence"] * 2,
            "year_id"     : [2010, 2011],
            "draw_0"      : [0.2] * 2,
            "draw_1"      : [0.3] * 2,
        }
    )
    canonical_iu_scenario_2 = pd.DataFrame(
        {
            "iu_name"     : ["AAA00001"] * 2,
            "scenario"    : ["scenario_2"] * 2,
            "country_code": ["AAA"] * 2,
            "measure"     : ["processed_prevalence"] * 2,
            "year_id"     : [2010, 2011],
            "draw_0"      : [0.8] * 2,
            "draw_1"      : [0.9] * 2,
        }
    )

    population_data_df = create_dummy_population_file_for_disease_with_years(Disease.LF, iu_yearly_population_map={
        "AAA00001": {
            2010: 10,
            2011: 15,
        }
    })

    canonical_ius = [canonical_iu_scenario_1, canonical_iu_scenario_2]
    population_data = IUData(input_data=population_data_df,
                             disease=Disease.LF,
                             iu_selection_criteria=IUSelectionCriteria.ALL_IUS)
    metadata_df = pd.DataFrame(
        {
            "year_id"     : [2010, 2011, 2010, 2011],
            "scenario"    : ["scenario_1", "scenario_1", "scenario_2", "scenario_2"],
            "country_code": ["AAA"] * 4,
            "measure"     : ["processed_prevalence"] * 4,
        }
    )

    prevalences = _compute_prevalences_in_country(canonical_ius, population_data)

    expected = metadata_df.copy()
    expected[prevalences.columns] = prevalences.values

    result = composite_run.build_composite_run_multiple_scenarios(
        canonical_ius,
        population_data,
    )
    pdt.assert_frame_equal(
        result,
        expected
    )


def _compute_prevalences_in_country(canonical_ius: List[pd.DataFrame],
                                    population_data: IUData):
    """
    Compute prevalence in a country by aggregating disease data across multiple Implementation Units (IUs).

    This function performs the following mathematical operations:

    Step 1: Extract draws from canonical IUs (3D array)
           
    all_ius_draws:
                        draws (columns)
                     [draw_0, draw_1, ..., draw_n]
           IU_0 ┌─┬─────────────────────────────┐
                │ │ prevalence values...        │ year_0
                │ ├─────────────────────────────┤
                │ │ prevalence values...        │ year_1
                │ ├─────────────────────────────┤
                │ │ ...                         │ ...
                │ └─────────────────────────────┘
           IU_1 ├─┬─────────────────────────────┐
                │ │ prevalence values...        │
                │ ├─────────────────────────────┤
                │ │ prevalence values...        │
                │ └─────────────────────────────┘
           ...  └─────────────────────────────────┘
           
    Shape: (num_IUs, num_years, num_draws)


    Step 2: Get priority populations (3D array with single column)

    populations:
           IU_0 ┌─┐
                │ │ pop_year_0
                │ │ pop_year_1
                │ │ ...
                └─┘
           IU_1 ┌─┐
                │ │ pop_year_0
                │ │ pop_year_1
                └─┘
           ...
           
    Shape: (num_IUs, num_years, 1)


    Step 3: Element-wise multiplication (broadcasting)

    case_numbers_across_ius = all_ius_draws * populations

           IU_0 ┌─┬─────────────────────────────┐
                │ │ cases = prev × pop          │ year_0
                │ ├─────────────────────────────┤
                │ │ cases = prev × pop          │ year_1
                │ └─────────────────────────────┘
           IU_1 ├─┬─────────────────────────────┐
                │ │ cases = prev × pop          │
                │ └─────────────────────────────┘
           ...
           
    Shape: (num_IUs, num_years, num_draws)


    Step 4: Sum across IUs (axis=0)

    case_numbers_in_country = np.sum(..., axis=0)

                ┌─────────────────────────────┐
                │ Σ(IU_0 + IU_1 + ... IU_n)   │ year_0
                ├─────────────────────────────┤
                │ Σ(IU_0 + IU_1 + ... IU_n)   │ year_1
                ├─────────────────────────────┤
                │ ...                         │ ...
                └─────────────────────────────┘
                
    Shape: (num_years, num_draws)


    Step 5: Divide by total population

    prevalence = case_numbers_in_country / total_population

    total_population:     Final prevalence:
    ┌─────────┐          ┌─────────────────────────────┐
    │ pop_y0  │          │ total_cases/total_pop       │ year_0
    │ pop_y1  │    →     │ total_cases/total_pop       │ year_1
    │ ...     │          │ ...                         │ ...
    └─────────┘          └─────────────────────────────┘

    Shape: (years, 1)     Shape: (num_years, num_draws)

    Args:
        canonical_ius: List of DataFrames containing prevalence data for each IU
        population_data: IUData object containing population metadata

    Returns:
        pd.DataFrame: Country-level prevalence data aggregated across all IUs
    """

    ius_org_by_scenario = itertools.groupby(canonical_ius, key=lambda iu: iu[canonical_columns.SCENARIO].iloc[0])

    result: List[pd.DataFrame] = []
    for _, ius in ius_org_by_scenario:
        ius_for_scenario = list(ius)

        draw_column_names, all_ius_draws = canonical_columns.extract_draws(ius_for_scenario)

        populations = np.array([
            population_data.get_priority_population_for_IU(iu[canonical_columns.IU_NAME].iloc[0])
            for iu in ius_for_scenario
        ]).reshape((len(ius_for_scenario), -1, 1))

        case_numbers_across_ius = all_ius_draws * populations

        case_numbers_in_country = np.sum(case_numbers_across_ius, axis=0)
        total_population = population_data.get_priority_population_for_country(
            ius_for_scenario[0][canonical_columns.COUNTRY_CODE].iloc[0]
        )

        if type(total_population) is dict:
            # population data is by year => in a given year what's the total population from all the IUs in the country
            total_population = np.array(list(total_population.values())).reshape((-1, 1))

        result.append(pd.DataFrame(
            case_numbers_in_country / total_population,
            columns=draw_column_names,
        ))

    return pd.concat(result, axis=0)
