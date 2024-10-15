from enum import Enum
import re

import pandas as pd

from endgame_postprocessing.post_processing.disease import Disease


def _is_valid_iu_code(iu_code):
    return re.match(r"[A-Z]{3}\d{5}$", iu_code)


def _get_capitalised_disease(disease: Disease):
    if disease is Disease.ONCHO:
        return "Oncho"
    elif disease is Disease.LF:
        return "LF"
    raise Exception(f"Invalid disease {disease}")


def preprocess_iu_meta_data(input_data: pd.DataFrame):
    deduped_input_data = input_data.drop_duplicates()
    new_iu_code = deduped_input_data.ADMIN0ISO3 + deduped_input_data["IU_ID"].apply(
        lambda id: str.zfill(str(id), 5)
    )
    deduped_input_data.loc[:, "IU_CODE"] = new_iu_code
    return deduped_input_data


class IUSelectionCriteria(Enum):
    ALL_IUS = 0
    MODELLED_IUS = 1


class IUData:

    def __init__(
        self,
        input_data: pd.DataFrame,
        disease: Disease,
        iu_selection_criteria: IUSelectionCriteria,
    ):
        self.disease = disease
        self.input_data = input_data
        self.iu_selection_criteria = iu_selection_criteria
        # TODO: validate the required columns are as expcted

        population_column_name = self._get_priority_population_column_name()
        if population_column_name not in input_data.columns:
            raise InvalidIUDataFile(
                f"No priority population found for disease {self.disease.name}"
                f", expected {population_column_name}"
            )

        if input_data["IU_CODE"].nunique() != len(input_data):
            raise InvalidIUDataFile("Duplicate IUs found")

        if (
            len(
                self.input_data[
                    self.input_data["IU_CODE"].apply(
                        lambda x: not bool(_is_valid_iu_code(x))
                    )
                ]
            )
            != 0
        ):
            raise InvalidIUDataFile("IU_CODE contains invalid IU codes")

    def get_priority_population_for_IU(self, iu_code):
        if not _is_valid_iu_code(iu_code):
            raise Exception(f"Invalid IU code: {iu_code}")
        iu = self.input_data.loc[self.input_data.IU_CODE == iu_code]
        if len(iu) == 0:
            raise Exception(f"IU {iu_code} not found in IU metadata file")
        assert len(iu) == 1
        return iu[self._get_priority_population_column_name()].iat[0]

    def get_priority_population_for_country(self, country_code):
        included_ius_in_country = self._get_included_ius_for_country(country_code)
        population_column = self._get_priority_population_column_name()
        return included_ius_in_country[population_column].sum()

    def get_priority_population_for_africa(self):
        return self.get_included_ius()[
            self._get_priority_population_column_name()
        ].sum()

    def get_total_ius_in_country(self, country_code):
        return len(self._get_included_ius_for_country(country_code))

    def _get_included_ius_for_country(self, country_code):
        return self.get_included_ius().loc[
            self.input_data["ADMIN0ISO3"] == country_code
        ]

    def get_included_ius(self):
        if self.iu_selection_criteria == IUSelectionCriteria.ALL_IUS:
            return self.input_data
        if self.iu_selection_criteria == IUSelectionCriteria.MODELLED_IUS:
            return self._get_modelled_ius()

    def _get_modelled_ius(self):
        modelled_column = self._get_modelled_column_name()
        return self.input_data[self.input_data[modelled_column]]

    def _get_priority_population_column_name(self):
        disease_str = _get_capitalised_disease(self.disease)
        return f"Priority_Population_{disease_str}"

    def _get_modelled_column_name(self):
        disease_str = _get_capitalised_disease(self.disease)
        return f"Modelled_{disease_str}"


class InvalidIUDataFile(Exception):
    pass
