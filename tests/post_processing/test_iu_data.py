import pandas as pd
import pytest
from endgame_postprocessing.post_processing.iu_data import (
    IUData,
    IUSelectionCriteria,
    InvalidIUDataFile,
)

from endgame_postprocessing.post_processing.disease import Disease
from tests.test_util.create_dummy_pop_file import create_dummy_population_file_for_disease, \
    create_dummy_population_file_for_disease_with_years, create_dummy_population_file


def test_iu_data_get_priority_population_iu_missing_raises_exception():
    with pytest.raises(Exception):
        metadata = create_dummy_population_file_for_disease(
            disease=Disease.LF,
            ius_population_map={},
            save_to_file=None,
        )
        IUData(metadata,
               disease=Disease.LF,
               iu_selection_criteria=IUSelectionCriteria.ALL_IUS).get_priority_population_for_IU("AAA00001")


def test_iu_data_without_valid_priority_population_column_raises_exception():
    with pytest.raises(Exception) as e:
        metadata = create_dummy_population_file_for_disease(Disease.LF, {})
        metadata.rename(columns={"Priority_Population_LF": "Priority_Population_InvalidDisease"}, inplace=True)

        IUData(
            metadata,
            disease=Disease.LF,
            iu_selection_criteria=IUSelectionCriteria.ALL_IUS,
        )
    assert e.match(
        "No priority population found for disease LF, expected Priority_Population_LF"
    )


def test_iu_data_get_priority_population_invalid_iu_raises_exception():
    with (pytest.raises(Exception)):
        IUData(pd.DataFrame({"IU_CODE": []}),
               disease=Disease.LF,
               iu_selection_criteria=IUSelectionCriteria.ALL_IUS).get_priority_population_for_IU("AAA0001")


def test_iu_data_get_priority_population_iu_in():
    assert (
            IUData(
                pd.DataFrame({"IU_CODE": ["AAA00001"], "Priority_Population_LF": [10]}),
                disease=Disease.LF,
                iu_selection_criteria=IUSelectionCriteria.ALL_IUS,
            ).get_priority_population_for_IU("AAA00001")
            == 10
    )


def test_iu_data_get_priority_population_iu_from_oncho_all_years():
    meta_data = create_dummy_population_file_for_disease_with_years(Disease.ONCHO, {
        "AAAXXXX00001": {1995: 10,
                         1996: 10}
    }, save_to_file=None)

    iudata = IUData(meta_data,
                    disease=Disease.ONCHO,
                    iu_selection_criteria=IUSelectionCriteria.ALL_IUS)
    yearly_population = iudata.get_priority_population_for_IU("AAAXXXX00001")
    assert len(yearly_population) > 0
    assert yearly_population[0] == 10
    assert yearly_population[1] == 10


def test_iu_data_get_priority_population_iu_from_oncho_specific_year():
    meta_data = create_dummy_population_file_for_disease_with_years(Disease.ONCHO, {
        "AAAXXXX00001": {1995: 10,
                         1996: 10}
    }, save_to_file=None)

    iudata = IUData(meta_data,
                    disease=Disease.ONCHO,
                    iu_selection_criteria=IUSelectionCriteria.ALL_IUS)
    yearly_population = iudata.get_priority_population_for_IU("AAAXXXX00001", 1996)
    assert yearly_population is not None
    assert yearly_population == 10


def test_iu_data_get_priority_population_iu_in_from_oncho():
    metadata = create_dummy_population_file({
        Disease.LF   : {"AAA00001": 10},
        Disease.ONCHO: {"AAA00001": 20}
    })

    assert (
            IUData(
                metadata,
                disease=Disease.ONCHO,
                iu_selection_criteria=IUSelectionCriteria.ALL_IUS,
            ).get_priority_population_for_IU("AAA00001")
            == 20
    )


def test_duplicate_iu_raises_exception():
    with pytest.raises(InvalidIUDataFile):
        IUData(
            pd.DataFrame(
                {
                    "IU_CODE"               : ["AAA00001", "AAA00001"],
                    "Priority_Population_LF": [10, 20],
                }
            ),
            disease=Disease.LF,
            iu_selection_criteria=IUSelectionCriteria.ALL_IUS,
        )


def test_iu_data_get_ius_in_country_one_iu_one_country():
    metadata = create_dummy_population_file({
        Disease.LF: {"AAA00001": 10}
    })

    assert (
            IUData(
                metadata,
                disease=Disease.LF,
                iu_selection_criteria=IUSelectionCriteria.ALL_IUS,
            ).get_total_ius_in_country("AAA")
            == 1
    )


def test_iu_data_get_ius_in_country_many_iu_one_country():
    metadata = create_dummy_population_file({
        Disease.LF: {"AAA00001": 10, "AAA00002": 10, "AAA00003": 10}
    })

    assert (
            IUData(
                metadata,
                disease=Disease.LF,
                iu_selection_criteria=IUSelectionCriteria.ALL_IUS,
            ).get_total_ius_in_country("AAA")
            == 3
    )


def test_iu_data_get_ius_in_country_only_modelled():
    assert (
            IUData(
                pd.DataFrame(
                    {
                        "ADMIN0ISO3"            : ["AAA"] * 3,
                        "IU_CODE"               : ["AAA00001", "AAA00002", "AAA00003"],
                        "Priority_Population_LF": [10] * 3,
                        "Modelled_LF"           : [True, False, False],
                    }
                ),
                disease=Disease.LF,
                iu_selection_criteria=IUSelectionCriteria.MODELLED_IUS,
            ).get_total_ius_in_country("AAA")
            == 1
    )


def test_iu_data_get_ius_in_country_only_endemic_lf():
    assert (
            IUData(
                pd.DataFrame(
                    {
                        "ADMIN0ISO3"            : ["AAA"] * 3,
                        "IU_CODE"               : ["AAA00001", "AAA00002", "AAA00003"],
                        "Priority_Population_LF": [10] * 3,
                        "Encemicity_LF"         : [
                            "Endemic (MDA not delivered)",
                            "Non-endemic",
                            "Non-endemic",
                        ],
                    }
                ),
                disease=Disease.LF,
                iu_selection_criteria=IUSelectionCriteria.ENDEMIC_IUS,
            ).get_total_ius_in_country("AAA")
            == 1
    )


def test_iu_data_get_ius_in_country_many_iu_many_country():
    metadata = create_dummy_population_file({
        Disease.LF: {"AAA00001": 10, "AAA00002": 10, "AAA00003": 10, "BBB00001": 10},
    })
    assert (
            IUData(
                metadata,
                disease=Disease.LF,
                iu_selection_criteria=IUSelectionCriteria.ALL_IUS,
            ).get_total_ius_in_country("AAA")
            == 3
    )


def test_iu_data_get_ius_in_country_many_iu_many_country_include_only_modelled():
    metadata = create_dummy_population_file({
        Disease.LF: {"AAA00001": 10, "AAA00002": 10, "AAA00003": 10, "BBB00001": 10},
    })
    metadata["Modelled_LF"] = [True, False, False, True]

    assert (
            IUData(
                metadata,
                disease=Disease.LF,
                iu_selection_criteria=IUSelectionCriteria.MODELLED_IUS,
            ).get_total_ius_in_country("AAA")
            == 1
    )


def test_get_population_for_country():
    metadata = create_dummy_population_file({
        Disease.LF: {
            "AAA00001": 100,
            "AAA00002": 200,
            "AAA00003": 300,
            "BBB00001": 400,
        }
    })
    assert (
            IUData(
                metadata,
                disease=Disease.LF,
                iu_selection_criteria=IUSelectionCriteria.ALL_IUS,
            ).get_priority_population_for_country("AAA")
            == 600
    )


def test_get_population_for_country_modelled_only():
    metadata = create_dummy_population_file({
        Disease.LF: {
            "AAA00001": 100,
            "AAA00002": 200,
            "AAA00003": 300,
            "BBB00001": 400,
        }
    })
    metadata["Modelled_LF"] = [True, False, False, True]
    assert (
            IUData(
                metadata,
                disease=Disease.LF,
                iu_selection_criteria=IUSelectionCriteria.MODELLED_IUS,
            ).get_priority_population_for_country("AAA")
            == 100
    )


def test_get_africa_population():
    metadata = create_dummy_population_file({
        Disease.LF: {"AAA00001": 100, "AAA00002": 200, "AAA00003": 300, "BBB00001": 400},
    })

    assert (
            IUData(
                metadata,
                disease=Disease.LF,
                iu_selection_criteria=IUSelectionCriteria.ALL_IUS,
            ).get_priority_population_for_africa()
            == 1000
    )


def test_get_africa_population_modelled_ius_only():
    metadata = create_dummy_population_file({
        Disease.LF: {"AAA00001": 100, "AAA00002": 200, "AAA00003": 300, "BBB00001": 400},
    })
    metadata["Modelled_LF"] = [True, False, False, True]

    assert (
            IUData(
                metadata,
                disease=Disease.LF,
                iu_selection_criteria=IUSelectionCriteria.MODELLED_IUS,
            ).get_priority_population_for_africa()
            == 500
    )


def test_get_africa_population_endemic_ius_only():
    metadata = create_dummy_population_file({
        Disease.ONCHO: {"AAA00001": 100, "AAA00002": 200, "AAA00003": 300, "BBB00001": 400},
    })
    metadata["Encemicity_Oncho"] = [
        "Endemic (MDA not delivered)",
        "Non-endemic",
        "Non-endemic",
        "Endemic (MDA not delivered)",
    ]

    assert (
            IUData(
                metadata,
                disease=Disease.ONCHO,
                iu_selection_criteria=IUSelectionCriteria.ENDEMIC_IUS,
            ).get_priority_population_for_africa()
            == 500
    )


def test_simulated_ius_includes_simulated_iu():
    metadata = create_dummy_population_file({
        Disease.ONCHO: {"AAA00001": 100, "AAA00002": 200, "AAA00003": 300, "BBB00001": 400},
    })
    assert (
            IUData(
                metadata,
                disease=Disease.ONCHO,
                iu_selection_criteria=IUSelectionCriteria.SIMULATED_IUS,
                simulated_IUs=["AAA00001", "BBB00001"],
            ).get_priority_population_for_africa()
            == 500
    )
