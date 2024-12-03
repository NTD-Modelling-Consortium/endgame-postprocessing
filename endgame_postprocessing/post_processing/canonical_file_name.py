from endgame_postprocessing.post_processing.dataclasses import CustomFileInfo


def get_name(file: CustomFileInfo):
    return f"{file.iu}_{file.scenario}_canonical.csv"


def get_regex():
    return r"(?P<iu_id>(?P<country>[A-Z]{3})\d{5})_(?P<scenario>scenario_\d+)_canonical.csv"
