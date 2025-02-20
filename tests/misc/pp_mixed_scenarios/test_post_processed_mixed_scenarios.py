import pytest
from endgame_postprocessing.post_processing.disease import Disease
from endgame_postprocessing.post_processing.pipeline_config import PipelineConfig
from misc.pp_mixed_scenarios.post_process_mixed_scenarios import (
    _get_pipeline_config_from_scenario_file,
)


def test_get_pipeline_config_from_scenario_file_no_threshold():
    mixed_scenarios_desc = {"disease": "oncho"}
    pipeline_config = _get_pipeline_config_from_scenario_file(mixed_scenarios_desc)
    assert pipeline_config == PipelineConfig(disease=Disease.ONCHO)


def test_get_pipeline_config_from_scenario_file_with_threshold():
    mixed_scenarios_desc = {"disease": "oncho", "threshold": 0.5}
    pipeline_config = _get_pipeline_config_from_scenario_file(mixed_scenarios_desc)
    assert pipeline_config == PipelineConfig(disease=Disease.ONCHO, threshold=0.5)


def test_get_pipeline_config_from_scenario_file_invalid_disease():
    mixed_scenarios_desc = {"disease": "typo"}
    with pytest.raises(
        Exception,
        match="Unexpected disease: typo, must be one of 'oncho', 'trachoma', 'lf'",
    ):
        _get_pipeline_config_from_scenario_file(mixed_scenarios_desc)


def test_get_pipeline_config_from_scenario_file_threshold_nan():
    mixed_scenarios_desc = {"disease": "oncho", "threshold": "hello"}
    with pytest.raises(
        Exception,
        match="threshold must be a number: .*",
    ):
        _get_pipeline_config_from_scenario_file(mixed_scenarios_desc)


def test_get_pipeline_config_from_scenario_file_threshold_below_range():
    mixed_scenarios_desc = {"disease": "oncho", "threshold": -0.1}
    with pytest.raises(
        Exception,
        match="threshold is -0.1, it must be between 0 and 1",
    ):
        _get_pipeline_config_from_scenario_file(mixed_scenarios_desc)


def test_get_pipeline_config_from_scenario_file_threshold_above_range():
    mixed_scenarios_desc = {"disease": "oncho", "threshold": 1.1}
    with pytest.raises(
        Exception,
        match="threshold is 1.1, it must be between 0 and 1",
    ):
        _get_pipeline_config_from_scenario_file(mixed_scenarios_desc)
