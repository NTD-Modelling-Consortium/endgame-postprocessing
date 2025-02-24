import re
from pathlib import Path
from typing import Dict, List

import pytest
import yaml

from endgame_postprocessing.post_processing.disease import Disease
from endgame_postprocessing.post_processing.pipeline_config import PipelineConfig
from misc.pp_mixed_scenarios.post_process_mixed_scenarios import (
    _get_pipeline_config_from_scenario_file,
    _find_duplicate_ius,
    _load_mixed_scenarios_desc,
    DuplicateIUError,
    MixedScenariosFileNotFound,
    InvalidInputDirectoryError,
    _validate_working_directory,
)


def test_get_pipeline_config_from_scenario_file_no_threshold():
    mixed_scenarios_desc = {"disease": "oncho"}
    pipeline_config = _get_pipeline_config_from_scenario_file(mixed_scenarios_desc)
    assert pipeline_config == PipelineConfig(disease=Disease.ONCHO)


def test_get_pipeline_config_from_scenario_file_with_threshold():
    mixed_scenarios_desc = {"disease": "oncho", "threshold": 0.5}
    pipeline_config = _get_pipeline_config_from_scenario_file(mixed_scenarios_desc)
    assert pipeline_config == PipelineConfig(disease=Disease.ONCHO, threshold=0.5)


def test_check_mixed_scenarios_desc_valid():
    try:
        _load_mixed_scenarios_desc(Path("./mixed_scenarios_desc_valid.yaml"))
    except Exception as e:
        pytest.fail(f"Test failed due to exception: {e}")


def make_duplicate_iu_report(iu_to_scenario_mapping: Dict[str, List[str]]):
    return [
        f"{iu} was duplicated in {' and '.join(sorted(scenarios))}"
        for iu, scenarios in iu_to_scenario_mapping.items()
    ]


def test_load_mixed_scenarios_desc_file_not_found():
    with pytest.raises(
        MixedScenariosFileNotFound,
        match=re.escape(str(MixedScenariosFileNotFound(Path("./non_existent_file.yaml")))),
    ):
        _load_mixed_scenarios_desc(Path("./non_existent_file.yaml"))


def test_check_mixed_scenarios_desc_duplicate_ius():
    with pytest.raises(
        DuplicateIUError,
        match=re.escape(str(DuplicateIUError({"CAF09661": ["scenario_1", "scenario_2"]}))),
    ):
        _load_mixed_scenarios_desc(Path("./mixed_scenarios_desc_duplicate_ius.yaml"))


def test_check_mixed_scenarios_desc_missing_default_scenario():
    root = Path.cwd().parent.parent.parent
    wd = root / "misc/pp_mixed_scenarios/examples/disease_1"

    mixed_scenarios_desc = yaml.load(
        Path("./mixed_scenarios_desc_missing_default_scenario.yaml").read_text(),
        Loader=yaml.FullLoader,
    )

    with pytest.raises(
        InvalidInputDirectoryError,
        match=re.escape(
            str(InvalidInputDirectoryError.missing_scenarios_from_specification(["scenario_a"]))
        ),
    ):
        input_directory = _validate_working_directory(
            wd,
            mixed_scenarios_desc,
        )

        assert input_directory == wd / "input"


def test_check_mixed_scenarios_desc_missing_overridden_scenarios():
    root = Path.cwd().parent.parent.parent
    wd = root / "misc/pp_mixed_scenarios/examples/disease_1"

    mixed_scenarios_desc = yaml.load(
        Path("./mixed_scenarios_desc_missing_scenarios.yaml").read_text(),
        Loader=yaml.FullLoader,
    )

    with pytest.raises(
        InvalidInputDirectoryError,
        match=re.escape(
            str(
                InvalidInputDirectoryError.missing_scenarios_from_specification(
                    ["scenario_1x", "scenario_2x"]
                )
            )
        ),
    ):
        input_directory = _validate_working_directory(
            wd,
            mixed_scenarios_desc,
        )

        assert input_directory == wd / "input"


def test_find_duplicate_ius():
    # Test Case 1: Basic duplicates within and across scenarios
    overridden_ius = {
        "scenario_1": ["IU_1", "IU_2", "IU_3", "IU_1"],
        "scenario_2": ["IU_2", "IU_4", "IU_5", "IU_2"],
        "scenario_3": ["IU_6", "IU_1", "IU_3", "IU_6"],
    }

    with pytest.raises(
        DuplicateIUError,
        match=re.escape(
            str(
                DuplicateIUError(
                    {
                        "IU_1": ["scenario_1", "scenario_3"],
                        "IU_2": ["scenario_1", "scenario_2"],
                        "IU_3": ["scenario_1", "scenario_3"],
                        "IU_6": ["scenario_3"],
                    }
                )
            )
        ),
    ):
        _find_duplicate_ius(overridden_ius)

    # Test Case 2: No duplicates
    overridden_ius = {
        "scenario_1": ["IU_1", "IU_2", "IU_3"],
        "scenario_2": ["IU_4", "IU_5", "IU_6"],
    }
    assert _find_duplicate_ius(overridden_ius) is None

    # Test Case 3: All duplicates are within scenario
    overridden_ius = {
        "scenario_1": ["IU_1", "IU_3", "IU_1"],
        "scenario_2": ["IU_2", "IU_4", "IU_5", "IU_2"],
    }

    expected_exception = DuplicateIUError({"IU_1": ["scenario_1"], "IU_2": ["scenario_2"]})
    with pytest.raises(DuplicateIUError, match=re.escape(str(expected_exception))):
        _find_duplicate_ius(overridden_ius)

    # Test Case 4: Empty dictionary
    assert _find_duplicate_ius({}) is None

    # Test Case 5: Case sensitivity
    overridden_ius = {"scenario_1": ["IU_1", "iU_1"], "scenario_2": ["IU_2", "iU_2"]}

    assert _find_duplicate_ius(overridden_ius) is None

    # Test Case 6: Complex case with multiple duplicates
    overridden_ius = {
        "scenario_1": ["IU_1", "IU_2", "IU_3", "IU_4", "IU_1", "IU_2"],
        "scenario_2": ["IU_3", "IU_5", "IU_6", "IU_3"],
        "scenario_3": ["IU_4", "IU_1", "IU_7", "IU_8", "IU_6", "IU_1"],
    }

    expected_exception = DuplicateIUError(
        {
            "IU_1": ["scenario_1", "scenario_3"],
            "IU_2": ["scenario_1"],
            "IU_3": ["scenario_1", "scenario_2"],
            "IU_4": ["scenario_1", "scenario_3"],
            "IU_6": ["scenario_2", "scenario_3"],
        }
    )

    with pytest.raises(DuplicateIUError, match=re.escape(str(expected_exception))):
        _find_duplicate_ius(overridden_ius)
