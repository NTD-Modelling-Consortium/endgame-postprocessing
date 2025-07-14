import re
from enum import Enum
from typing import Optional

import pandas as pd
from numpy.f2py.cfuncs import includes

from endgame_postprocessing.post_processing import canonical_columns
from endgame_postprocessing.post_processing.disease import Disease
from endgame_postprocessing.post_processing.endemicity_classification import (
    ENDEMICITY_CLASSIFIERS,
)


def _is_valid_iu_code(iu_code):
    return re.match(r"[A-Z]{3}.{0,5}[\d]{5}$", iu_code)


def _get_capitalised_disease(disease: Disease):
    if disease is Disease.ONCHO:
        return "Oncho"
    elif disease is Disease.LF:
        return "LF"
    elif disease is Disease.STH:
        return "STH"
    elif disease is Disease.SCH:
        return "Schisto"
    elif disease is Disease.TRACHOMA:
        return "Trachoma"
    raise Exception(f"Invalid disease {disease}")


def _get_priority_population_column_for_disease(disease: Disease):
    return f"Priority_Population_{_get_capitalised_disease(disease)}"


class IUSelectionCriteria(Enum):
    ALL_IUS = 0
    MODELLED_IUS = 1
    ENDEMIC_IUS = 2
    SIMULATED_IUS = 4  # ie the ones this post processing script is running against


class IUData:

    def __init__(
            self,
            input_data: pd.DataFrame,
            disease: Disease,
            iu_selection_criteria: IUSelectionCriteria,
            simulated_IUs: set[str] = None,
    ):
        self.disease = disease
        self.input_data = input_data
        self.iu_selection_criteria = iu_selection_criteria
        self.simulated_IUs = simulated_IUs
        self.has_yearly_data = "Year" in input_data.columns
        if iu_selection_criteria is IUSelectionCriteria.SIMULATED_IUS:
            assert simulated_IUs is not None
        # TODO: validate the required columns are as expcted

        population_column_name = _get_priority_population_column_for_disease(
            self.disease
        )
        if population_column_name not in input_data.columns:
            raise InvalidIUDataFile(
                f"No priority population found for disease {self.disease.name}"
                f", expected {population_column_name}"
            )

        if "Year" in input_data.columns:
            if input_data.duplicated(subset=["IU_CODE", "Year"]).any():
                raise InvalidIUDataFile("Duplicate IU and Year combination found")
        else:
            if input_data["IU_CODE"].duplicated().any():
                raise InvalidIUDataFile("Duplicate IUs found")

        if (len(
                self.input_data[
                    self.input_data["IU_CODE"].apply(
                        lambda x: not bool(_is_valid_iu_code(x))
                    )
                ]
        ) != 0):
            raise InvalidIUDataFile("IU_CODE contains invalid IU codes")

    def get_priority_population_for_IU(self, iu_code: str, year: Optional[int] = None):
        """
        Fetches the priority population for a specific IU (Implementation Unit) and optionally, a specific year.
    
        Args:
            iu_code (str): The code representing the Implementation Unit (IU).
            year (Optional[int]): The year for which the priority population is requested. If not provided,
                                  the method returns data for all available years.
    
        Returns:
            int or list: The priority population for the specified IU and year, or a list of populations for
                         all years if the year is not specified.
    
        Raises:
            Exception: If the IU code is invalid or if the IU is not found in the meta data file.
        """

        if not _is_valid_iu_code(iu_code):
            raise Exception(f"Invalid IU code: {iu_code}")
        iu: pd.Series = self.input_data.loc[self.input_data.IU_CODE == iu_code]
        priority_population_column = _get_priority_population_column_for_disease(self.disease)
        if len(iu) == 0:
            # Consider using the preprocess_iu_meta_data function to add in every simulated IU into
            # the meta data file
            raise Exception(f"Could not find IU {iu_code} in the IU meta data file")

        if self.has_yearly_data:
            if year is not None:
                return iu.loc[self.input_data["Year"] == year, priority_population_column].iat[0]

            # return the populations for all the years
            return iu.loc[:, priority_population_column].tolist()

        if len(iu) == 1:
            return iu[priority_population_column].iat[0]

    def get_priority_population_for_country(self, country_code):
        included_ius_in_country: pd.DataFrame = self._get_included_ius_for_country(country_code)
        population_column = _get_priority_population_column_for_disease(self.disease)
        if not self.has_yearly_data:
            return included_ius_in_country[population_column].sum()

        years = included_ius_in_country["Year"].unique()
        return {
            year: included_ius_in_country[included_ius_in_country["Year"] == year][
                population_column].sum()
            for year in years
        }

    def get_priority_population_for_africa(self):
        included_ius = self.get_included_ius()
        population_column = _get_priority_population_column_for_disease(self.disease)
        if not self.has_yearly_data:
            return included_ius[population_column].sum()

        years = included_ius["Year"].unique()
        return {
            year: included_ius[included_ius["Year"] == year][population_column].sum()
            for year in years
        }

    def get_total_ius_in_country(self, country_code):
        included_ius_in_country = self._get_included_ius_for_country(country_code)
        if not self.has_yearly_data:
            return len(included_ius_in_country)
        else:
            # For yearly data, count unique IU_CODEs, not total rows
            return len(included_ius_in_country["IU_CODE"].unique())

    def _get_included_ius_for_country(self, country_code):
        return self.get_included_ius().loc[self.input_data["ADMIN0ISO3"] == country_code]

    def get_included_ius(self):
        if self.iu_selection_criteria == IUSelectionCriteria.ALL_IUS:
            return self.input_data
        if self.iu_selection_criteria == IUSelectionCriteria.MODELLED_IUS:
            return self._get_modelled_ius()
        if self.iu_selection_criteria == IUSelectionCriteria.ENDEMIC_IUS:
            return self._get_endemic_ius()
        if self.iu_selection_criteria == IUSelectionCriteria.SIMULATED_IUS:
            return self._get_simulated_ius()
        raise Exception(f"Invalid IU Selection Criteria {self.iu_selection_criteria}")

    def _get_modelled_ius(self):
        modelled_column = self._get_modelled_column_name()
        return self.input_data[self.input_data[modelled_column]]

    def _get_modelled_column_name(self):
        disease_str = _get_capitalised_disease(self.disease)
        return f"Modelled_{disease_str}"

    def _get_simulated_ius(self):
        return self.input_data.loc[self.input_data["IU_CODE"].isin(self.simulated_IUs)]

    def _get_endemic_ius(self):
        endemic_column = self._get_endemic_column_name()
        endemicity_classifier = ENDEMICITY_CLASSIFIERS[self.disease]
        return self.input_data.loc[
            self.input_data[endemic_column].apply(
                endemicity_classifier.is_state_endemic
            )
        ]

    def _get_endemic_column_name(self):
        disease_str = _get_capitalised_disease(self.disease)
        # TODO: typo in the column name - should push into the preprocess step
        return f"Encemicity_{disease_str}"


class InvalidIUDataFile(Exception):
    pass
