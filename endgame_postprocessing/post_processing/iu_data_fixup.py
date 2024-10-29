import warnings
import pandas as pd

from endgame_postprocessing.post_processing.disease import Disease
from endgame_postprocessing.post_processing.iu_data import (
    _get_priority_population_column_for_disease,
    _is_valid_iu_code,
)


DEFAULT_POPULATION = 10000.0


def remove_non_simulated_ius(input_data: pd.DataFrame, simulated_IUs: set[str]):
    return input_data.loc[input_data.IU_CODE.isin(simulated_IUs)]


def insert_missing_ius(
    input_data: pd.DataFrame, simulated_IUs: set[str]
) -> pd.DataFrame:
    assert all([_is_valid_iu_code(iu_code) for iu_code in simulated_IUs])

    ordered_ius = list(simulated_IUs)

    required_ius_data = pd.DataFrame(
        {
            "IU_CODE": ordered_ius,
            "ADMIN0ISO3": [iu_code[0:3] for iu_code in ordered_ius],
        }
    )
    missing_ius = required_ius_data[~required_ius_data.IU_CODE.isin(input_data.IU_CODE)]
    if len(missing_ius) > 0:
        warnings.warn(
            f"{len(missing_ius)} were missing from the meta data file: {missing_ius.loc[:, 'IU_CODE'].values}"  # noqa 501
        )
        warnings.warn(
            f"For these IUs a default population of {DEFAULT_POPULATION} will be used"  # noqa 501
        )
    input_data_with_all_ius = pd.merge(
        input_data, required_ius_data, how="outer", on=["IU_CODE", "ADMIN0ISO3"]
    )

    population_columns = [
        _get_priority_population_column_for_disease(disease) for disease in Disease
    ]

    return input_data_with_all_ius.fillna(
        {column_name: DEFAULT_POPULATION for column_name in population_columns}
    )


def fixup_iu_meta_data_file(input_data: pd.DataFrame, simulated_IUs: set[str]):
    deduped_input_data = input_data.drop_duplicates()
    new_iu_code = deduped_input_data.ADMIN0ISO3 + deduped_input_data["IU_ID"].apply(
        lambda id: str.zfill(str(id), 5)
    )
    deduped_input_data.loc[:, "IU_CODE"] = new_iu_code

    # We add in all simulated IUs into the IU meta data file so that every IU
    # has a population, and country and continent level populations can be calculated
    all_simulated_ius = insert_missing_ius(deduped_input_data, simulated_IUs)

    # We remove non-simualted IUs as the current IU meta data file has mismatched IUs
    # so we don't really know if the IUs that don't have matching simulations are
    # actually simulated with a different ID, so for now we just drop them
    only_simulated_ius = remove_non_simulated_ius(all_simulated_ius, simulated_IUs)
    return only_simulated_ius
