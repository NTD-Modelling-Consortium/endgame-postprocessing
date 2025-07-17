import itertools
from typing import List
from pathlib import Path

import numpy as np
import pandas as pd
import pandas.testing as pdt

from endgame_postprocessing.post_processing import composite_run, canonical_columns
from endgame_postprocessing.post_processing.disease import Disease
from endgame_postprocessing.post_processing.iu_data import IUData, IUSelectionCriteria
from tests.test_util.create_dummy_pop_file import (
    create_dummy_population_file_for_disease_with_years, 
    create_dummy_population_file_for_disease,
    create_population_metadata_file_with_yearly_data
)


def test_trim_canonical_ius_to_year_range_perfect_overlap():
    """Test trimming canonical IUs with perfect year range overlap."""
    canonical_ius = [pd.DataFrame({
        "iu_name": ["AAA00001"] * 3,
        "year_id": [2010, 2011, 2012],
        "scenario": ["scenario_1"] * 3,
        "country_code": ["AAA"] * 3,
        "measure": ["processed_prevalence"] * 3,
        "draw_0": [0.1, 0.2, 0.3],
    })]
    
    from endgame_postprocessing.post_processing.composite_run import _trim_canonical_ius_to_year_range
    result = _trim_canonical_ius_to_year_range(canonical_ius, (2010, 2012))
    
    assert len(result) == 1
    assert len(result[0]) == 3
    assert list(result[0]["year_id"]) == [2010, 2011, 2012]


def test_trim_canonical_ius_to_year_range_partial_trim():
    """Test trimming canonical IUs that removes some years."""
    canonical_ius = [pd.DataFrame({
        "iu_name": ["AAA00001"] * 5,
        "year_id": [2008, 2009, 2010, 2011, 2012],
        "scenario": ["scenario_1"] * 5,
        "country_code": ["AAA"] * 5,
        "measure": ["processed_prevalence"] * 5,
        "draw_0": [0.1, 0.2, 0.3, 0.4, 0.5],
    })]
    
    from endgame_postprocessing.post_processing.composite_run import _trim_canonical_ius_to_year_range
    result = _trim_canonical_ius_to_year_range(canonical_ius, (2010, 2012))
    
    assert len(result) == 1
    assert len(result[0]) == 3
    assert list(result[0]["year_id"]) == [2010, 2011, 2012]
    assert list(result[0]["draw_0"]) == [0.3, 0.4, 0.5]


def test_trim_canonical_ius_to_year_range_no_overlap():
    """Test trimming canonical IUs with no year overlap."""
    canonical_ius = [pd.DataFrame({
        "iu_name": ["AAA00001"] * 2,
        "year_id": [2000, 2001],
        "scenario": ["scenario_1"] * 2,
        "country_code": ["AAA"] * 2,
        "measure": ["processed_prevalence"] * 2,
        "draw_0": [0.1, 0.2],
    })]
    
    from endgame_postprocessing.post_processing.composite_run import _trim_canonical_ius_to_year_range
    result = _trim_canonical_ius_to_year_range(canonical_ius, (2010, 2012))
    
    assert len(result) == 0


def test_build_composite_run_with_year_mismatch_partial_overlap():
    """Test build_composite_run handles partial year overlap by trimming simulation data."""
    # Simulation data spans 2008-2012, but metadata only covers 2010-2012
    canonical_ius = [pd.DataFrame({
        "iu_name": ["AAA00001"] * 5,
        "year_id": [2008, 2009, 2010, 2011, 2012],
        "scenario": ["scenario_1"] * 5,
        "country_code": ["AAA"] * 5,
        "measure": ["processed_prevalence"] * 5,
        "draw_0": [0.1, 0.2, 0.3, 0.4, 0.5],
        "draw_1": [0.15, 0.25, 0.35, 0.45, 0.55],
    })]
    
    population_data = IUData(
        create_dummy_population_file_for_disease_with_years(Disease.LF, {
            "AAA00001": {2010: 100, 2011: 110, 2012: 120}
        }),
        Disease.LF,
        IUSelectionCriteria.ALL_IUS
    )
    
    result = composite_run.build_composite_run(canonical_ius, population_data)
    
    # Should only contain years 2010-2012 (the intersection)
    assert len(result) == 3
    assert list(result["year_id"]) == [2010, 2011, 2012]
    
    # Check that the data corresponds to the trimmed years (not the original first years)
    expected_prevalences = pd.DataFrame({
        "draw_0": [0.3, 0.4, 0.5],  # Original values for years 2010-2012
        "draw_1": [0.35, 0.45, 0.55],
    })
    
    pdt.assert_frame_equal(
        result[["draw_0", "draw_1"]].reset_index(drop=True),
        expected_prevalences,
        check_dtype=False
    )


def test_build_composite_run_with_year_mismatch_no_overlap_raises_error():
    """Test build_composite_run raises error when no year overlap exists."""
    # Simulation data spans 2000-2002, but metadata covers 2010-2012
    canonical_ius = [pd.DataFrame({
        "iu_name": ["AAA00001"] * 3,
        "year_id": [2000, 2001, 2002],
        "scenario": ["scenario_1"] * 3,
        "country_code": ["AAA"] * 3,
        "measure": ["processed_prevalence"] * 3,
        "draw_0": [0.1, 0.2, 0.3],
        "draw_1": [0.15, 0.25, 0.35],
    })]
    
    population_data = IUData(
        create_dummy_population_file_for_disease_with_years(Disease.LF, {
            "AAA00001": {2010: 100, 2011: 110, 2012: 120}
        }),
        Disease.LF,
        IUSelectionCriteria.ALL_IUS
    )
    
    import pytest
    with pytest.raises(ValueError, match="No overlap between simulation years and population metadata years"):
        composite_run.build_composite_run(canonical_ius, population_data)


def test_build_composite_run_with_metadata_extends_beyond_simulation():
    """Test build_composite_run when metadata covers more years than simulation."""
    # Simulation data spans 2010-2011, but metadata covers 2008-2015
    canonical_ius = [pd.DataFrame({
        "iu_name": ["AAA00001"] * 2,
        "year_id": [2010, 2011],
        "scenario": ["scenario_1"] * 2,
        "country_code": ["AAA"] * 2,
        "measure": ["processed_prevalence"] * 2,
        "draw_0": [0.3, 0.4],
        "draw_1": [0.35, 0.45],
    })]
    
    population_data = IUData(
        create_dummy_population_file_for_disease_with_years(Disease.LF, {
            "AAA00001": {2008: 80, 2009: 90, 2010: 100, 2011: 110, 2012: 120, 2013: 130, 2014: 140, 2015: 150}
        }),
        Disease.LF,
        IUSelectionCriteria.ALL_IUS
    )
    
    result = composite_run.build_composite_run(canonical_ius, population_data)
    
    # Should contain all simulation years since they're all covered by metadata
    assert len(result) == 2
    assert list(result["year_id"]) == [2010, 2011]
    
    # Data should be unchanged since no trimming was needed
    expected_prevalences = pd.DataFrame({
        "draw_0": [0.3, 0.4],
        "draw_1": [0.35, 0.45],
    })
    
    pdt.assert_frame_equal(
        result[["draw_0", "draw_1"]].reset_index(drop=True),
        expected_prevalences,
        check_dtype=False
    )


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
    population_data = IUData(create_dummy_population_file_for_disease(Disease.LF,
                                                                      {"AAA00001": 100}), Disease.LF,
                             iu_selection_criteria=IUSelectionCriteria.ALL_IUS)

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
    population_data = IUData(create_dummy_population_file_for_disease(Disease.LF,
                                                                      {
                                                                          "AAA00001": 100,
                                                                          "AAA00002": 0,
                                                                      }), Disease.LF,
                             iu_selection_criteria=IUSelectionCriteria.ALL_IUS)

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
    population_data = IUData(create_dummy_population_file_for_disease(Disease.LF,
                                                                      {
                                                                          "AAA00001": 10,
                                                                          "AAA00002": 10,
                                                                      }), disease=Disease.LF,
                             iu_selection_criteria=IUSelectionCriteria.ALL_IUS)

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
    iu_metadata = IUData(input_data=create_dummy_population_file_for_disease_with_years(
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
    ), disease=Disease.LF, iu_selection_criteria=IUSelectionCriteria.ALL_IUS)

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
    population_data = IUData(pd.DataFrame(
        {
            "IU_CODE"               : ["AAA00001"],
            "ADMIN0ISO3"            : ["AAA"],
            "Priority_Population_LF": [10],
        }
    ), disease=Disease.LF, iu_selection_criteria=IUSelectionCriteria.ALL_IUS)
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
    population_data = IUData(input_data=population_data_df, disease=Disease.LF,
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

        populations = []
        num_years = ius_for_scenario[0][canonical_columns.YEAR_ID].nunique()
        
        for iu in ius_for_scenario:
            iu_code = iu[canonical_columns.IU_NAME].iloc[0]
            pop_iterator = population_data.get_priority_population_for_iu(iu_code)
            populations.append(list(itertools.islice(pop_iterator, num_years)))
        
        populations = np.array(populations).reshape((len(ius_for_scenario), -1, 1))

        case_numbers_across_ius = all_ius_draws * populations

        case_numbers_in_country = np.sum(case_numbers_across_ius, axis=0)
        total_population_iter = population_data.get_priority_population_for_country(
            ius_for_scenario[0][canonical_columns.COUNTRY_CODE].iloc[0]
        )

        # Use islice to get the right number of years for both longitudinal and non-longitudinal data
        total_population = np.array(list(itertools.islice(total_population_iter, num_years))).reshape((-1, 1))

        result.append(pd.DataFrame(
            case_numbers_in_country / total_population,
            columns=draw_column_names,
        ))

    return pd.concat(result, axis=0)


def test_compute_year_range_intersection_longitudinal_data_perfect_overlap():
    """Test year range intersection with perfect overlap between simulation and metadata."""
    canonical_ius = [pd.DataFrame({
        "iu_name": ["AAA00001"] * 3,
        "year_id": [2010, 2011, 2012],
        "scenario": ["scenario_1"] * 3,
        "country_code": ["AAA"] * 3,
        "measure": ["processed_prevalence"] * 3,
        "draw_0": [0.1, 0.2, 0.3],
    })]
    
    population_data = IUData(
        create_dummy_population_file_for_disease_with_years(Disease.LF, {
            "AAA00001": {2010: 100, 2011: 110, 2012: 120}
        }),
        Disease.LF,
        IUSelectionCriteria.ALL_IUS
    )
    
    from endgame_postprocessing.post_processing.composite_run import _compute_year_range_intersection
    result = _compute_year_range_intersection(canonical_ius, population_data)
    
    assert result == (2010, 2012)


def test_compute_year_range_intersection_longitudinal_data_partial_overlap():
    """Test year range intersection with partial overlap - simulation extends beyond metadata."""
    canonical_ius = [pd.DataFrame({
        "iu_name": ["AAA00001"] * 5,
        "year_id": [2008, 2009, 2010, 2011, 2012],
        "scenario": ["scenario_1"] * 5,
        "country_code": ["AAA"] * 5,
        "measure": ["processed_prevalence"] * 5,
        "draw_0": [0.1, 0.2, 0.3, 0.4, 0.5],
    })]
    
    population_data = IUData(
        create_dummy_population_file_for_disease_with_years(Disease.LF, {
            "AAA00001": {2010: 100, 2011: 110, 2012: 120}
        }),
        Disease.LF,
        IUSelectionCriteria.ALL_IUS
    )
    
    from endgame_postprocessing.post_processing.composite_run import _compute_year_range_intersection
    result = _compute_year_range_intersection(canonical_ius, population_data)
    
    assert result == (2010, 2012)


def test_compute_year_range_intersection_longitudinal_data_metadata_extends_beyond():
    """Test year range intersection where metadata extends beyond simulation."""
    canonical_ius = [pd.DataFrame({
        "iu_name": ["AAA00001"] * 2,
        "year_id": [2010, 2011],
        "scenario": ["scenario_1"] * 2,
        "country_code": ["AAA"] * 2,
        "measure": ["processed_prevalence"] * 2,
        "draw_0": [0.1, 0.2],
    })]
    
    population_data = IUData(
        create_dummy_population_file_for_disease_with_years(Disease.LF, {
            "AAA00001": {2008: 80, 2009: 90, 2010: 100, 2011: 110, 2012: 120}
        }),
        Disease.LF,
        IUSelectionCriteria.ALL_IUS
    )
    
    from endgame_postprocessing.post_processing.composite_run import _compute_year_range_intersection
    result = _compute_year_range_intersection(canonical_ius, population_data)
    
    assert result == (2010, 2011)


def test_compute_year_range_intersection_longitudinal_data_no_overlap():
    """Test year range intersection with no overlap between simulation and metadata."""
    canonical_ius = [pd.DataFrame({
        "iu_name": ["AAA00001"] * 2,
        "year_id": [2000, 2001],
        "scenario": ["scenario_1"] * 2,
        "country_code": ["AAA"] * 2,
        "measure": ["processed_prevalence"] * 2,
        "draw_0": [0.1, 0.2],
    })]
    
    population_data = IUData(
        create_dummy_population_file_for_disease_with_years(Disease.LF, {
            "AAA00001": {2010: 100, 2011: 110, 2012: 120}
        }),
        Disease.LF,
        IUSelectionCriteria.ALL_IUS
    )
    
    from endgame_postprocessing.post_processing.composite_run import _compute_year_range_intersection
    result = _compute_year_range_intersection(canonical_ius, population_data)
    
    assert result is None


def test_compute_year_range_intersection_non_longitudinal_data():
    """Test year range intersection with non-longitudinal metadata data."""
    canonical_ius = [pd.DataFrame({
        "iu_name": ["AAA00001"] * 3,
        "year_id": [2010, 2011, 2012],
        "scenario": ["scenario_1"] * 3,
        "country_code": ["AAA"] * 3,
        "measure": ["processed_prevalence"] * 3,
        "draw_0": [0.1, 0.2, 0.3],
    })]
    
    population_data = IUData(
        create_dummy_population_file_for_disease(Disease.LF, {"AAA00001": 100}),
        Disease.LF,
        IUSelectionCriteria.ALL_IUS
    )
    
    from endgame_postprocessing.post_processing.composite_run import _compute_year_range_intersection
    result = _compute_year_range_intersection(canonical_ius, population_data)
    
    # For non-longitudinal data, should return the simulation year range
    assert result == (2010, 2012)


def test_compute_year_range_intersection_empty_canonical_ius():
    """Test year range intersection with empty canonical IUs list."""
    canonical_ius = []
    
    population_data = IUData(
        create_dummy_population_file_for_disease(Disease.LF, {"AAA00001": 100}),
        Disease.LF,
        IUSelectionCriteria.ALL_IUS
    )
    
    from endgame_postprocessing.post_processing.composite_run import _compute_year_range_intersection
    result = _compute_year_range_intersection(canonical_ius, population_data)
    
    assert result is None


def test_oncho_longitudinal_population_data_conversion_pipeline(fs):
    """Test the entire pipeline with real longitudinal oncho population data conversion."""
    
    # Create a subset of real longitudinal population data in memory
    raw_population_data = pd.DataFrame({
        'IU_ID': [10697] * 10 + [10698] * 10,  # Two IUs
        'year_id': list(range(2010, 2020)) * 2,  # Years 2010-2019 for both IUs
        'sex_id': [3] * 20,  # Both sexes
        'ihme_loc_id': ['SDN'] * 10 + ['ETH'] * 10,  # Sudan and Ethiopia
        'location_id': [522] * 10 + [179] * 10,
        'location_name': ['Sudan'] * 10 + ['Ethiopia'] * 10,
        'sex': ['both'] * 20,
        'adj_pop': [
            # Sudan IU populations (growing over time)
            88493, 90430, 92140, 94033, 96285, 98749, 101348, 104130, 106975, 109601,
            # Ethiopia IU populations (different growth pattern)
            75000, 77000, 79000, 81000, 83000, 85000, 87000, 89000, 91000, 93000
        ]
    })
    
    # Create fake files in the fake filesystem
    fs.create_file("/tmp/raw_oncho_data.csv")
    raw_population_data.to_csv("/tmp/raw_oncho_data.csv", index=False)
    
    # Convert to PopulationMetadatafile.csv format using the utility function
    population_metadata_df = create_population_metadata_file_with_yearly_data(
        raw_data_csv=Path("/tmp/raw_oncho_data.csv"),
        iuid_column="IU_ID",
        country_code_column="ihme_loc_id", 
        year_column="year_id",
        population_column="adj_pop",
        disease=Disease.ONCHO,
        save_to_file=None  # Don't save to file, just return DataFrame
    )
    
    # Create IUData object from the converted population metadata
    iu_data = IUData(population_metadata_df, Disease.ONCHO, IUSelectionCriteria.ALL_IUS)
    
    # Verify that it correctly identifies as longitudinal data
    assert iu_data.is_longitudinal
    
    # Verify year range
    year_range = iu_data.year_range
    assert year_range['min'] == 2010
    assert year_range['max'] == 2019
    
    # Create canonical simulation data that partially overlaps with population data
    # Simulation spans 2008-2015, population data spans 2010-2019
    # Expected intersection: 2010-2015
    canonical_ius = [pd.DataFrame({
        "iu_name": ["SDN10697"] * 8,
        "year_id": [2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015],
        "scenario": ["scenario_1"] * 8,
        "country_code": ["SDN"] * 8,
        "measure": ["processed_prevalence"] * 8,
        "draw_0": [0.1, 0.15, 0.2, 0.25, 0.3, 0.35, 0.4, 0.45],
        "draw_1": [0.12, 0.17, 0.22, 0.27, 0.32, 0.37, 0.42, 0.47],
    }), pd.DataFrame({
        "iu_name": ["ETH10698"] * 8,
        "year_id": [2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015],
        "scenario": ["scenario_1"] * 8,
        "country_code": ["ETH"] * 8,
        "measure": ["processed_prevalence"] * 8,
        "draw_0": [0.05, 0.08, 0.1, 0.12, 0.15, 0.18, 0.2, 0.22],
        "draw_1": [0.06, 0.09, 0.11, 0.13, 0.16, 0.19, 0.21, 0.23],
    })]
    
    # Test individual country processing with year mismatch handling
    expected_years = [2010, 2011, 2012, 2013, 2014, 2015]
    
    # Test Sudan only first
    sudan_canonical_ius = [canonical_ius[0]]
    sudan_result = composite_run.build_composite_run(sudan_canonical_ius, iu_data)
    
    # Verify the result contains only the intersection years
    assert len(sudan_result) == len(expected_years)
    assert list(sudan_result["year_id"]) == expected_years
    
    # Verify that the prevalence values correspond to the trimmed simulation data
    # (years 2010-2015, not 2008-2009) - these should match exactly for single IU
    expected_prevalences_draw_0 = [0.2, 0.25, 0.3, 0.35, 0.4, 0.45]  # From 2010-2015
    assert list(sudan_result["draw_0"]) == expected_prevalences_draw_0
    
    # Test Ethiopia only
    ethiopia_canonical_ius = [canonical_ius[1]]
    ethiopia_result = composite_run.build_composite_run(ethiopia_canonical_ius, iu_data)
    
    assert len(ethiopia_result) == len(expected_years)
    assert list(ethiopia_result["year_id"]) == expected_years
    expected_ethiopia_prevalences_draw_0 = [0.1, 0.12, 0.15, 0.18, 0.2, 0.22]  # From 2010-2015
    assert list(ethiopia_result["draw_0"]) == expected_ethiopia_prevalences_draw_0
    
    # Test multi-scenario processing with trimming
    canonical_ius_multi_scenario = [
        # Scenario 1 - Sudan only
        pd.DataFrame({
            "iu_name": ["SDN10697"] * 8,
            "year_id": [2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015],
            "scenario": ["scenario_1"] * 8,
            "country_code": ["SDN"] * 8,
            "measure": ["processed_prevalence"] * 8,
            "draw_0": [0.1, 0.15, 0.2, 0.25, 0.3, 0.35, 0.4, 0.45],
            "draw_1": [0.12, 0.17, 0.22, 0.27, 0.32, 0.37, 0.42, 0.47],
        }),
        # Scenario 2 - Sudan only (different values)
        pd.DataFrame({
            "iu_name": ["SDN10697"] * 8,
            "year_id": [2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015],
            "scenario": ["scenario_2"] * 8,
            "country_code": ["SDN"] * 8,
            "measure": ["processed_prevalence"] * 8,
            "draw_0": [0.08, 0.12, 0.16, 0.2, 0.24, 0.28, 0.32, 0.36],
            "draw_1": [0.09, 0.13, 0.17, 0.21, 0.25, 0.29, 0.33, 0.37],
        })
    ]
    
    multi_scenario_result = composite_run.build_composite_run_multiple_scenarios(
        canonical_ius_multi_scenario, iu_data
    )
    
    # Should have data for both scenarios, each trimmed to 2010-2015
    scenarios = multi_scenario_result["scenario"].unique()
    assert "scenario_1" in scenarios
    assert "scenario_2" in scenarios
    
    # Each scenario should have 6 years (trimmed from 2010-2015)
    scenario_1_data = multi_scenario_result[multi_scenario_result["scenario"] == "scenario_1"]
    scenario_2_data = multi_scenario_result[multi_scenario_result["scenario"] == "scenario_2"]
    
    assert len(scenario_1_data) == 6  # 6 years for scenario_1
    assert len(scenario_2_data) == 6  # 6 years for scenario_2
    
    # Verify trimming worked - should have years 2010-2015, not 2008-2009
    assert list(scenario_1_data["year_id"]) == expected_years
    assert list(scenario_2_data["year_id"]) == expected_years
    
    # Verify correct prevalence values after trimming
    assert list(scenario_1_data["draw_0"]) == [0.2, 0.25, 0.3, 0.35, 0.4, 0.45]
    assert list(scenario_2_data["draw_0"]) == [0.16, 0.2, 0.24, 0.28, 0.32, 0.36]
