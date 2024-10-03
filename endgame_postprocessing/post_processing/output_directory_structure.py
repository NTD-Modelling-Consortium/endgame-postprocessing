from pathlib import Path

import pandas as pd

from endgame_postprocessing.post_processing.dataclasses import CustomFileInfo


def write_canonical(
    root_dir, file_info: CustomFileInfo, canonical_result: pd.DataFrame
):
    scenario = file_info.scenario
    country = file_info.country
    iu = file_info.iu
    file_name = f"{iu}_canonical.csv"
    path = Path(f"{get_canonical_dir(root_dir)}/{scenario}/{country}/{iu}/")
    path.mkdir(parents=True, exist_ok=True)
    canonical_result.to_csv(f"{path}/{file_name}")


def get_canonical_dir(working_dir):
    return f"{working_dir}/canonical_results/"


def write_iu_stat_agg(
    root_dir, file_info: CustomFileInfo, iu_statistical_aggregate: pd.DataFrame
):
    scenario = file_info.scenario
    iu = file_info.iu
    file_name = f"{scenario}_{iu}_post_processed.csv"
    path = Path(f"{root_dir}/ius/")
    path.mkdir(parents=True, exist_ok=True)
    iu_statistical_aggregate.to_csv(f"{path}/{file_name}")


def write_country_stat_agg(root_dir, country_statistical_aggregate: pd.DataFrame):
    file_name = f"combined-country-lvl-agg.csv"
    path = Path(f"{root_dir}/aggregated/")
    path.mkdir(parents=True, exist_ok=True)
    country_statistical_aggregate.to_csv(f"{path}/{file_name}")


def write_country_composite(root_dir, country: str, country_composite: pd.DataFrame):
    file_name = f"{country}_composite.csv"
    path = Path(f"{root_dir}/composite/")
    path.mkdir(parents=True, exist_ok=True)
    country_composite.to_csv(f"{path}/{file_name}")


def write_africa_composite(root_dir, country_composite: pd.DataFrame):
    file_name = "africa_composite.csv"
    path = Path(f"{root_dir}/composite/")
    path.mkdir(parents=True, exist_ok=True)
    country_composite.to_csv(f"{path}/{file_name}")

def write_africa_stat_agg(root_dir, africa_statistical_aggregate: pd.DataFrame):
    file_name = "combined-africa-lvl-agg.csv"
    path = Path(f"{root_dir}/aggregated/")
    path.mkdir(parents=True, exist_ok=True)
    africa_statistical_aggregate.to_csv(f"{path}/{file_name}")
