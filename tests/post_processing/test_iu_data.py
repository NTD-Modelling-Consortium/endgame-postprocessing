import itertools
from pathlib import Path

import more_itertools
import pandas as pd
import pytest
import pandas.testing as pdt
from pyfakefs.fake_filesystem import FakeFilesystem

from endgame_postprocessing.post_processing.iu_data import (
    IUData,
    IUSelectionCriteria,
    InvalidIUDataFile,
)

from endgame_postprocessing.post_processing.disease import Disease
from tests.test_util.create_dummy_pop_file import create_dummy_population_file_for_disease, \
    create_dummy_population_file_for_disease_with_years, create_dummy_population_file, \
    create_population_metadata_file_with_yearly_data


def test_iu_data_get_priority_population_iu_missing_raises_exception():
    with pytest.raises(Exception):
        metadata = create_dummy_population_file_for_disease(
            disease=Disease.LF,
            ius_population_map={},
            save_to_file=None,
        )
        IUData(metadata, disease=Disease.LF,
               iu_selection_criteria=IUSelectionCriteria.ALL_IUS).get_priority_population_for_iu("AAA00001")


def test_iu_data_without_valid_priority_population_column_raises_exception():
    with pytest.raises(Exception) as e:
        metadata = create_dummy_population_file_for_disease(Disease.LF, {})
        metadata.rename(columns={"Priority_Population_LF": "Priority_Population_InvalidDisease"}, inplace=True)

        IUData(metadata, disease=Disease.LF, iu_selection_criteria=IUSelectionCriteria.ALL_IUS)
    assert e.match(
        "No priority population found for disease LF, expected Priority_Population_LF"
    )


def test_iu_data_get_priority_population_invalid_iu_raises_exception():
    with (pytest.raises(Exception)):
        IUData(pd.DataFrame({"IU_CODE": []}), disease=Disease.LF,
               iu_selection_criteria=IUSelectionCriteria.ALL_IUS).get_priority_population_for_iu("AAA0001")


def test_iu_data_get_priority_population_for_iu():
    meta_data = IUData(
        create_dummy_population_file_for_disease(
            disease=Disease.LF,
            ius_population_map={"AAA00001": 10},
            save_to_file=None,
        ),
        disease=Disease.LF,
        iu_selection_criteria=IUSelectionCriteria.ALL_IUS
    )
    population = meta_data.get_priority_population_for_iu("AAA00001")

    assert meta_data.is_longitudinal is False
    assert 10 == next(population)


def test_iu_data_get_priority_population_iu_from_oncho_all_years():
    iudata = IUData(
        create_dummy_population_file_for_disease_with_years(Disease.ONCHO,
                                                            {
                                                                "AAAXXXX00001": {1995: 10,
                                                                                 1996: 10}
                                                            }, save_to_file=None),
        disease=Disease.ONCHO,
        iu_selection_criteria=IUSelectionCriteria.ALL_IUS)
    yearly_population = list(iudata.get_priority_population_for_iu("AAAXXXX00001"))

    assert iudata.is_longitudinal is True
    assert len(yearly_population) == 2
    assert yearly_population[0] == 10
    assert yearly_population[1] == 10


def test_iu_data_get_priority_population_iu_from_oncho_specific_year():
    meta_data = create_dummy_population_file_for_disease_with_years(Disease.ONCHO, {
        "AAAXXXX00001": {1995: 10,
                         1996: 10}
    }, save_to_file=None)

    iudata = IUData(meta_data, disease=Disease.ONCHO, iu_selection_criteria=IUSelectionCriteria.ALL_IUS)
    yearly_population = iudata.get_priority_population_for_iu("AAAXXXX00001", 1996)
    assert iudata.is_longitudinal is True
    assert more_itertools.only(yearly_population) == 10


def test_iu_data_get_priority_population_iu_in_from_oncho():
    metadata = create_dummy_population_file({
        Disease.LF   : {"AAA00001": 10},
        Disease.ONCHO: {"AAA00001": 20}
    })

    iudata = IUData(metadata, disease=Disease.ONCHO,
                    iu_selection_criteria=IUSelectionCriteria.ALL_IUS)
    population = iudata.get_priority_population_for_iu("AAA00001")
    assert iudata.is_longitudinal is False
    assert more_itertools.first(population) == 20


def test_duplicate_iu_raises_exception():
    with pytest.raises(InvalidIUDataFile):
        IUData(pd.DataFrame(
            {
                "IU_CODE"               : ["AAA00001", "AAA00001"],
                "Priority_Population_LF": [10, 20],
            }
        ), disease=Disease.LF, iu_selection_criteria=IUSelectionCriteria.ALL_IUS)


def test_iu_data_count_ius_in_country_one_iu_one_country():
    metadata = create_dummy_population_file({
        Disease.LF: {"AAA00001": 10}
    })

    assert (
            IUData(metadata, disease=Disease.LF,
                   iu_selection_criteria=IUSelectionCriteria.ALL_IUS).get_total_ius_in_country("AAA")
            == 1
    )


def test_iu_data_count_ius_in_country_one_iu_one_country_yearly_population():
    metadata = create_dummy_population_file_for_disease_with_years(
        disease=Disease.ONCHO,
        iu_yearly_population_map={
            "AAA00001": {1995: 10, 1996: 20},
            "BBB00001": {1995: 30, 1996: 40},
        }, save_to_file=None)

    assert (
            IUData(metadata, disease=Disease.ONCHO,
                   iu_selection_criteria=IUSelectionCriteria.ALL_IUS).get_total_ius_in_country("AAA")
            == 1
    )


def test_iu_data_count_ius_in_country_many_iu_one_country():
    metadata = create_dummy_population_file({
        Disease.LF: {
            "AAA00001": 10,
            "AAA00002": 10,
            "AAA00003": 10,
        }
    })

    assert (
            IUData(metadata, disease=Disease.LF,
                   iu_selection_criteria=IUSelectionCriteria.ALL_IUS).get_total_ius_in_country("AAA")
            == 3
    )


def test_iu_data_count_ius_in_country_many_iu_one_country_yearly_population():
    metadata = create_dummy_population_file_for_disease_with_years(
        disease=Disease.LF,
        iu_yearly_population_map={
            "AAA00001": {
                1995: 10,
                1996: 20,
            },
            "AAA00002": {
                1995: 30,
                1996: 40,
            },
            "AAA00003": {
                1995: 50,
                1996: 60,
            }
        },
        save_to_file=None,
    )

    assert (
            IUData(metadata, disease=Disease.LF,
                   iu_selection_criteria=IUSelectionCriteria.ALL_IUS).get_total_ius_in_country("AAA")
            == 3
    )


def test_iu_data_get_ius_in_country_only_modelled():
    assert (
            IUData(pd.DataFrame(
                {
                    "ADMIN0ISO3"            : ["AAA"] * 3,
                    "IU_CODE"               : ["AAA00001", "AAA00002", "AAA00003"],
                    "Priority_Population_LF": [10] * 3,
                    "Modelled_LF"           : [True, False, False],
                }
            ), disease=Disease.LF,
                iu_selection_criteria=IUSelectionCriteria.MODELLED_IUS).get_total_ius_in_country("AAA")
            == 1
    )


def test_iu_data_get_ius_in_country_only_endemic_lf():
    assert (
            IUData(pd.DataFrame(
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
            ), disease=Disease.LF,
                iu_selection_criteria=IUSelectionCriteria.ENDEMIC_IUS).get_total_ius_in_country("AAA")
            == 1
    )


def test_iu_data_count_ius_in_country_many_iu_many_country():
    metadata = create_dummy_population_file({
        Disease.LF: {
            "AAA00001": 10,
            "AAA00002": 10,
            "AAA00003": 10,
            "BBB00001": 10
        },
    })
    assert (
            IUData(metadata, disease=Disease.LF,
                   iu_selection_criteria=IUSelectionCriteria.ALL_IUS).get_total_ius_in_country("AAA")
            == 3
    )


def test_iu_data_count_ius_in_country_many_iu_many_country_include_only_modelled():
    metadata = create_dummy_population_file({
        Disease.LF: {"AAA00001": 10, "AAA00002": 10, "AAA00003": 10, "BBB00001": 10},
    })
    metadata["Modelled_LF"] = [True, False, False, True]

    assert (
            IUData(metadata, disease=Disease.LF,
                   iu_selection_criteria=IUSelectionCriteria.MODELLED_IUS).get_total_ius_in_country("AAA")
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
            next(IUData(metadata, disease=Disease.LF,
                   iu_selection_criteria=IUSelectionCriteria.ALL_IUS).get_priority_population_for_country("AAA"))
            == 600
    )


def test_get_population_for_country_yearly():
    metadata = create_dummy_population_file_for_disease_with_years(
        Disease.LF,
        {
            "AAA00001": {1995: 100, 1996: 200},
            "AAA00002": {1995: 300, 1996: 400},
            "AAA00003": {1995: 500, 1996: 600},
            "BBB00001": {1995: 700, 1996: 800},
        }
    )
    iu_data = IUData(metadata, Disease.LF, IUSelectionCriteria.ALL_IUS)
    population_data = list(iu_data.get_priority_population_for_country("AAA"))

    assert type(population_data) == list
    assert population_data == [
        sum([100, 300, 500]),  # 1995
        sum([200, 400, 600]),  # 1996
    ]


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
            next(IUData(metadata, disease=Disease.LF,
                   iu_selection_criteria=IUSelectionCriteria.MODELLED_IUS).get_priority_population_for_country("AAA"))
            == 100
    )


def test_get_africa_population():
    metadata = create_dummy_population_file({
        Disease.LF: {"AAA00001": 100,
                     "AAA00002": 200,
                     "AAA00003": 300,
                     "BBB00001": 400},
    })

    assert (
            next(IUData(metadata, disease=Disease.LF,
                   iu_selection_criteria=IUSelectionCriteria.ALL_IUS).get_priority_population_for_africa())
            == 1000
    )


def test_get_africa_population_yearly():
    iu_yearly_population_map = {
        "AAA00001": {1995: 100, 1996: 200},
        "AAA00002": {1995: 300, 1996: 400},
        "AAA00003": {1995: 500, 1996: 600},
        "BBB00001": {1995: 700, 1996: 800},
    }

    metadata = create_dummy_population_file_for_disease_with_years(
        disease=Disease.LF,
        iu_yearly_population_map=iu_yearly_population_map,
    )

    iu_data = IUData(metadata, disease=Disease.LF, iu_selection_criteria=IUSelectionCriteria.ALL_IUS)
    population_data = list(iu_data.get_priority_population_for_africa())
    assert type(population_data) == list
    assert population_data == [
        sum([100, 300, 500, 700]),  # 1995
        sum([200, 400, 600, 800]),  # 1996
    ]


def test_get_africa_population_modelled_ius_only():
    metadata = create_dummy_population_file({
        Disease.LF: {"AAA00001": 100, "AAA00002": 200, "AAA00003": 300, "BBB00001": 400},
    })
    metadata["Modelled_LF"] = [True, False, False, True]

    assert (
            next(IUData(metadata, disease=Disease.LF,
                   iu_selection_criteria=IUSelectionCriteria.MODELLED_IUS).get_priority_population_for_africa())
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
            next(IUData(metadata, disease=Disease.ONCHO,
                   iu_selection_criteria=IUSelectionCriteria.ENDEMIC_IUS).get_priority_population_for_africa())
            == 500
    )


def test_simulated_ius_includes_simulated_iu():
    metadata = create_dummy_population_file({
        Disease.ONCHO: {"AAA00001": 100, "AAA00002": 200, "AAA00003": 300, "BBB00001": 400},
    })
    assert (
            next(IUData(metadata, disease=Disease.ONCHO, iu_selection_criteria=IUSelectionCriteria.SIMULATED_IUS,
                   simulated_ius=["AAA00001", "BBB00001"]).get_priority_population_for_africa())
            == 500
    )


def test_create_yearly_population_metadatafile_from_raw_data(fs: FakeFilesystem):
    raw_data_contents = {
        "IU_ID"      : ["1", "1", "1", "1", "2", "2", "2", "2"],
        "year_id"    : [1995, 1995, 1996, 1996, 1995, 1995, 1996, 1996],
        "adj_pop"    : [10, 5, 15, 7, 20, 12, 25, 13],
        "ihme_loc_id": ["AAA", "AAA", "AAA", "AAA", "BBB", "BBB", "BBB", "BBB"],
        "sex"        : ["both", "male", "both", "female", "both", "male", "both", "female"],
    }

    path_to_raw_data = Path("raw_data.csv")
    fs.create_file(file_path=path_to_raw_data,
                   contents=pd.DataFrame(raw_data_contents).to_csv(index=False))
    result_df = create_population_metadata_file_with_yearly_data(path_to_raw_data,
                                                                 iuid_column="IU_ID",
                                                                 country_code_column="ihme_loc_id",
                                                                 year_column="year_id",
                                                                 population_column="adj_pop",
                                                                 disease=Disease.ONCHO,
                                                                 save_to_file=None)

    # Verify the data matches what we expect after filtering for sex="both"
    raw_data_df = pd.read_csv(path_to_raw_data, usecols=["IU_ID", "year_id", "adj_pop", "ihme_loc_id", "sex"])
    raw_data_df.sort_values(by=["IU_ID", "year_id"], inplace=True, ascending=[True, True])
    raw_data_df = raw_data_df[raw_data_df["sex"] == "both"]
    raw_data_df.drop(columns=["sex"], inplace=True)
    raw_data_df.rename(columns={
        "ihme_loc_id": "ADMIN0ISO3",
        "year_id"    : "Year",
        "adj_pop"    : "Priority_Population_Oncho",
    }, inplace=True)

    match_cols = list(raw_data_df.columns)
    match_cols.remove("IU_ID")
    pdt.assert_frame_equal(result_df[match_cols], raw_data_df[match_cols])
