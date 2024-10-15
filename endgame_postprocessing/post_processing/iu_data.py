import re

import pandas as pd

from endgame_postprocessing.post_processing.disease import Disease


def _is_valid_iu_code(iu_code):
    return re.match("[A-Z]{3}\d{5}$", iu_code)


class IUData:

    def __init__(self, input_data: pd.DataFrame, disease: Disease):
        self.disease = disease
        # TODO: validate the required columns are as expcted

        if self._get_priority_population_column_name() not in input_data.columns:
            raise InvalidIUDataFile(
                f"No priority population found for disease {self.disease}, expected {self._get_priority_population_column_name()}"
            )

        if input_data["IU_CODE"].nunique() != len(input_data):
            raise InvalidIUDataFile("Duplicate IUs found")

        self.input_data = input_data

    def get_priority_population_for_IU(self, iu_code):
        if not _is_valid_iu_code(iu_code):
            raise Exception(f"Invalid IU code: {iu_code}")
        iu = self.input_data.loc[self.input_data.IU_CODE == iu_code]
        if len(iu) == 0:
            raise Exception(f"IU {iu_code} not found in IU metadata file")
        assert len(iu) == 1
        return iu[self._get_priority_population_column_name()].iat[0]

    def get_priority_population_for_country(self, country_code):
        iu_codes = self._get_iu_codes_for_country(country_code)

        return sum(self.get_priority_population_for_IU(iu_code) for iu_code in iu_codes)

    def get_priority_population_for_africa(self):
        return sum(
            self.get_priority_population_for_IU(iu_code)
            for iu_code in self.input_data["IU_CODE"]
        )

    def get_total_ius_in_country(self, country_code):
        return len(self._get_iu_codes_for_country(country_code))

    def _get_iu_codes_for_country(self, country_code):
        return self.input_data[self.input_data["ADMIN0ISO3"] == country_code]["IU_CODE"]

    def _get_priority_population_column_name(self):
        if self.disease is Disease.ONCHO:
            disease_str = "Oncho"
        elif self.disease is Disease.LF:
            disease_str = "LF"
        else:
            raise Exception(f"Invalid disease {self.disease}")
        return f"Priority_Population_{disease_str}"

    # TODO: implement get_modelled_ius_for_country, get_endemic_ius_for_country


class InvalidIUDataFile(Exception):
    pass
