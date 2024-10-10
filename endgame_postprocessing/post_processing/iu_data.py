import re

import pandas as pd


def _is_valid_iu_code(iu_code):
    return re.match("[A-Z]{3}\d{5}", iu_code)


class IUData:

    def __init__(self, input_data: pd.DataFrame):
        if input_data["IU_CODE"].nunique() != len(input_data):
            raise InvalidIUDataFile("Duplicate IUs found")
        self.input_data = input_data

    def get_priority_population_for_IU(self, iu_code):
        if not _is_valid_iu_code(iu_code):
            raise Exception(f"Invalid IU code: {iu_code}")
        iu = self.input_data.loc[self.input_data.IU_CODE == iu_code]
        if len(iu) == 0:
            # raise Exception(f"IU {iu_code} not found in IU metadata file")
            return 10000
        assert len(iu) == 1
        return iu["population"].iat[0]


class InvalidIUDataFile(Exception):
    pass
