import re


def _is_valid_iu_code(iu_code):
    return re.match("[A-Z]{3}\d{5}", iu_code)


class IUData:

    def __init__(self, input_data):
        self.input_data = input_data

    def get_priority_population_for_IU(self, iu_code):
        if not _is_valid_iu_code(iu_code):
            raise Exception(f"Invalid IU code: {iu_code}")
        if iu_code in self.input_data:
            return self.input_data[iu_code]
        return 10000
