from enum import Enum
import re
import warnings

import pandas as pd

from endgame_postprocessing.post_processing.disease import Disease
from endgame_postprocessing.post_processing.endemicity_classification import (
    ENDEMICITY_CLASSIFIERS,
)


def _is_valid_iu_code(iu_code):
    return re.match(r"[A-Z]{3}\d{5}$", iu_code)


def _get_capitalised_disease(disease: Disease):
    if disease is Disease.ONCHO:
        return "Oncho"
    elif disease is Disease.LF:
        return "LF"
    elif disease is Disease.STH:
        return "STH"
    elif disease is Disease.SCH:
        return "Schisto"
    raise Exception(f"Invalid disease {disease}")


def _get_priority_population_column_for_disease(disease: Disease):
    return f"Priority_Population_{_get_capitalised_disease(disease)}"
def remove_non_simulated_ius(input_data: pd.DataFrame, simulated_IUs: list[str]):
    return input_data.loc[input_data.IU_CODE.isin(simulated_IUs)]


def insert_missing_ius(
    input_data: pd.DataFrame, required_ius: list[str]
) -> pd.DataFrame:
    assert all([_is_valid_iu_code(iu_code) for iu_code in required_ius])

    required_ius_data = pd.DataFrame(
        {
            "IU_CODE": required_ius,
            "ADMIN0ISO3": [iu_code[0:3] for iu_code in required_ius],
        }
    ).drop_duplicates()
    missing_ius = required_ius_data[~required_ius_data.IU_CODE.isin(input_data.IU_CODE)]
    if len(missing_ius) > 0:
        warnings.warn(
            f"{len(missing_ius)} were missing from the meta data file: {missing_ius.loc[:, 'IU_CODE'].values}"
        )
    input_data_with_all_ius = pd.merge(
        input_data, required_ius_data, how="outer", on=["IU_CODE", "ADMIN0ISO3"]
    )

    population_columns = [
        _get_priority_population_column_for_disease(disease) for disease in Disease
    ]

    return input_data_with_all_ius.fillna(
        {column_name: 10000.0 for column_name in population_columns}
    )


def preprocess_iu_meta_data(input_data: pd.DataFrame, simulated_IUs: list[str]):
    deduped_input_data = input_data.drop_duplicates()
    new_iu_code = deduped_input_data.ADMIN0ISO3 + deduped_input_data["IU_ID"].apply(
        lambda id: str.zfill(str(id), 5)
    )
    deduped_input_data.loc[:, "IU_CODE"] = new_iu_code
    all_simulated_ius = insert_missing_ius(deduped_input_data, simulated_IUs)

    # We remove non-simualted IUs as the current IU meta data file has mismatched IUs
    # so we don't really know if the IUs that don't have matching simulations are
    # actually simulated with a different ID, so for now we just drop them
    only_simulated_ius = remove_non_simulated_ius(all_simulated_ius, simulated_IUs)
    return only_simulated_ius


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
        simulated_IUs: list[str] = None,
    ):
        self.disease = disease
        self.input_data = input_data
        self.iu_selection_criteria = iu_selection_criteria
        self.simulated_IUs = simulated_IUs
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
            warnings.warn(
                f"Could not find IU {iu_code} in the IU meta data file, using population of 10000"
            )
            return 10000
        assert len(iu) == 1
        return iu[_get_priority_population_column_for_disease(self.disease)].iat[0]

    def get_priority_population_for_country(self, country_code):
        included_ius_in_country = self._get_included_ius_for_country(country_code)
        population_column = _get_priority_population_column_for_disease(self.disease)
        return included_ius_in_country[population_column].sum()

    def get_priority_population_for_africa(self):
        return self.get_included_ius()[
            _get_priority_population_column_for_disease(self.disease)
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
