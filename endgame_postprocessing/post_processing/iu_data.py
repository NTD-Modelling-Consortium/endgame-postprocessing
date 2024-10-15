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


class IUData:

    def __init__(self, input_data: pd.DataFrame, disease: Disease):
        self.disease = disease
        self.input_data = input_data
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
        return self._get_ius_for_country(country_code)[
            self._get_priority_population_column_name()
        ].sum()

    def get_priority_population_for_africa(self):
        return self.input_data[self._get_priority_population_column_name()].sum()

    def get_total_ius_in_country(self, country_code):
        return len(self._get_ius_for_country(country_code))

    def _get_ius_for_country(self, country_code):
        return self.input_data[self.input_data["ADMIN0ISO3"] == country_code]

    def _get_priority_population_column_name(self):
        disease_str = _get_capitalised_disease(self.disease)
        return f"Priority_Population_{disease_str}"

    # TODO: implement get_modelled_ius_for_country, get_endemic_ius_for_country


class InvalidIUDataFile(Exception):
    pass
