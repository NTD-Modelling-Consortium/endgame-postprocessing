from pathlib import Path
from typing import Dict, Optional, Union, TypeAlias

import more_itertools as miter
import pandas as pd

from endgame_postprocessing.post_processing import iu_data
from endgame_postprocessing.post_processing.disease import Disease

DEFAULT_POPULATION_SIZE = 10000

PopulationSize = int
IUPopulationMap = Dict[str, PopulationSize]
YearlyPopulationMap = Dict[int, PopulationSize]
IUYearlyPopulationMap = Dict[str, YearlyPopulationMap]
IUPopulationData = IUPopulationMap | IUYearlyPopulationMap

DiseasePopulationData: TypeAlias = Dict[Disease, IUPopulationData]


def create_dummy_population_file(disease_data: DiseasePopulationData,
                                 save_to_file: Optional[Path] = "PopulationMetadatafile.csv") -> pd.DataFrame:
    """
    Create a dummy population metadata file for multiple diseases, supporting both yearly and non-yearly population data.

    Args:
        disease_data: A dictionary mapping Disease to a dictionary of IU_CODE -> population data.
                        Population data can be either:
                        - An integer (for a single population size)
                        - A dictionary mapping years to population sizes
        save_to_file: Optional path to save the file to (None to skip saving)

    Returns:
        pandas DataFrame containing the population data

    Example:
        # Single disease, no years
        create_dummy_population_file({
            Disease.LF: {"AAA00001": 10000, "BBB00001": 15000}
        })

        # Multiple diseases, no years
        create_dummy_population_file({
            Disease.LF: {"AAA00001": 10000, "BBB00001": 15000},
            Disease.ONCHO: {"AAA00001": 8000, "BBB00001": 12000}
        })

        # Single disease with yearly data
        create_dummy_population_file({
            Disease.LF: {
                "AAA00001": {2000: 10000, 2001: 15000},
                "BBB00001": {2000: 10000, 2001: 15000}
            }
        })
    """
    if not disease_data:
        raise ValueError("At least one disease must be provided")

    has_yearly_data = False
    for iu_pop_map in disease_data.values():
        for pop_data in iu_pop_map.values():
            if isinstance(pop_data, dict):
                has_yearly_data = True
                break
        if has_yearly_data:
            break

    if has_yearly_data:
        return _create_with_yearly_info(disease_data, save_to_file)

    return _create_without_yearly_info(disease_data, save_to_file)


def _create_without_yearly_info(disease_data: DiseasePopulationData,
                                save_to_file: Optional[Path] = "PopulationMetadatafile.csv") -> pd.DataFrame:
    iu_codes = set()
    for iu_pop_map in disease_data.values():
        iu_codes.update(iu_pop_map.keys())

    iu_codes = sorted(iu_codes)

    meta_data = pd.DataFrame({
        "IU_CODE"   : iu_codes,
        "IU_ID"     : [iuc[-5:] for iuc in iu_codes],
        "ADMIN0ISO3": [iuc[:3] for iuc in iu_codes],
    })

    for disease, iu_pop_map in disease_data.items():
        priority_pop_col_name = f"Priority_Population_{iu_data._get_capitalised_disease(disease)}"
        meta_data[priority_pop_col_name] = pd.NA

        for iu_code, population in iu_pop_map.items():
            meta_data.loc[meta_data["IU_CODE"] == iu_code, priority_pop_col_name] = population

        meta_data[priority_pop_col_name].convert_dtypes(convert_integer=True)

    if save_to_file is not None:
        meta_data.to_csv(save_to_file, index=False)

    return meta_data


def _create_with_yearly_info(disease_data: DiseasePopulationData,
                             save_to_file: Optional[Path] = "PopulationMetadatafile.csv") -> pd.DataFrame:
    iu_ypm: IUYearlyPopulationMap = miter.first(list(disease_data.values()))
    iu_codes = sorted(set(iu_ypm.keys()))
    years = sorted(set(miter.first(list(iu_ypm.values())).keys()))

    rows = []
    for iu_code in iu_codes:
        iu_id = iu_code[-5:]
        admin0iso3 = iu_code[:3]

        for disease, iu_pop_map in disease_data.items():
            priority_pop_col_name = f"Priority_Population_{iu_data._get_capitalised_disease(disease)}"
            yearly_pop_sizes = iu_pop_map.get(iu_code, {})

            for year in years:
                row = {
                    "IU_CODE"   : iu_code,
                    "IU_ID"     : iu_id,
                    "ADMIN0ISO3": admin0iso3,
                    "Year"      : year
                }

                pop_size = yearly_pop_sizes.get(year, pd.NA)
                if pop_size is not pd.NA:
                    pop_size = int(pop_size)
                row[priority_pop_col_name] = pop_size
                rows.append(row)

    meta_data = pd.DataFrame(rows)

    if save_to_file is not None:
        meta_data.to_csv(save_to_file, index=False)

    return meta_data


def create_dummy_population_file_for_disease(disease: Disease,
                                             ius_population_map: IUPopulationMap,
                                             save_to_file: Optional[Path] = "PopulationMetadatafile.csv"):
    """Create a simple, non-yearly, population file for a single disease.
    Notes:
        Exists for backwards compatibility.
    """
    return create_dummy_population_file({disease: ius_population_map}, save_to_file)


def create_dummy_population_file_for_disease_with_years(disease: Disease,
                                                        iu_yearly_population_map: IUYearlyPopulationMap,
                                                        save_to_file: Optional[Path] = "PopulationMetadatafile.csv"):
    """Create a population file with yearly sizes for every IU, and a single disease.

    Notes:
        Exists for backwards compatibility.
    """
    return create_dummy_population_file({disease: iu_yearly_population_map}, save_to_file)
