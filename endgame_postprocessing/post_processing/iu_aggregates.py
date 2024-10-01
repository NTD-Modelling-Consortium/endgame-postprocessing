from pathlib import Path
import pandas as pd
from endgame_postprocessing.post_processing.dataclasses import CustomFileInfo


def write_iu_aggregate(
    root_dir, file_info: CustomFileInfo, canonical_result: pd.DataFrame
):
    scenario = file_info.scenario
    iu = file_info.iu
    file_name = f"{scenario}_{iu}_post_processed.csv"
    path = Path(f"{root_dir}/ius/")
    path.mkdir(parents=True, exist_ok=True)
    canonical_result.to_csv(f"{path}/{file_name}")
