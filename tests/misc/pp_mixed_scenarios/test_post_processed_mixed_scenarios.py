import itertools
import pprint
import re
from pathlib import Path

import pytest
import yaml

from endgame_postprocessing.post_processing.disease import Disease
from endgame_postprocessing.post_processing.pipeline_config import PipelineConfig
from misc.pp_mixed_scenarios.exceptions import (
    MissingScenariosFromSpecificationError,
    MissingPopulationMetadataFileError,
    MissingCanonicalResultsDirectoryError,
    InvalidDiseaseFieldError,
    InvalidThresholdError,
    InvalidOverriddenIUsError,
    MissingFieldsError,
)
from misc.pp_mixed_scenarios.post_process_mixed_scenarios import (
    _get_pipeline_config_from_scenario_file,
    _find_duplicate_ius,
    _load_mixed_scenarios_desc,
    DuplicateIUError,
    MixedScenariosFileNotFound,
    _validate_working_directory,
    MixedScenariosDescription,
    _collect_source_target_paths,
    _prepare_output_directory,
)


def test_get_pipeline_config_from_scenario_file_no_threshold():
    mixed_scenarios_desc = MixedScenariosDescription.from_dict(
        {
            "disease": "oncho",
            "default_scenario": "scenario_1",
            "overridden_ius": {},
            "scenario_name": "scenario_x1",
        }
    )
    pipeline_config = _get_pipeline_config_from_scenario_file(mixed_scenarios_desc)
    assert pipeline_config == PipelineConfig(disease=Disease.ONCHO)


def test_get_pipeline_config_from_scenario_file_with_threshold():
    mixed_scenarios_desc = MixedScenariosDescription.from_dict(
        {
            "disease": "oncho",
            "threshold": 0.5,
            "default_scenario": "scenario_1",
            "overridden_ius": {},
            "scenario_name": "scenario_x1",
        }
    )
    pipeline_config = _get_pipeline_config_from_scenario_file(mixed_scenarios_desc)
    assert pipeline_config == PipelineConfig(disease=Disease.ONCHO, threshold=0.5)


def test_check_mixed_scenarios_desc_invalid_disease_field_error(fs):
    spec_yaml = Path("mixed_scenarios_invalid_disease.yaml")
    fs.create_file(
        spec_yaml,
        contents=yaml.dump(
            data={
                "disease": "invalid_disease",
                "default_scenario": "scenario_0",
                "overridden_ius": {
                    "scenario_1": ["CAF09661", "CAF09662"],
                    "scenario_2": ["CAF09663"],
                },
                "scenario_name": "scenario_x1",
            }
        ),
    )

    with pytest.raises(InvalidDiseaseFieldError) as exc_info:
        _load_mixed_scenarios_desc(spec_yaml)

    assert exc_info.value.disease == "invalid_disease"
    assert exc_info.value.valid_set == {"oncho", "lf", "trachoma"}


def test_check_mixed_scenarios_desc_invalid_threshold_error(fs):
    spec_yaml = Path("mixed_scenarios_invalid_threshold.yaml")
    fs.create_file(
        spec_yaml,
        contents=yaml.dump(
            {
                "disease": "lf",
                "default_scenario": "scenario_0",
                "overridden_ius": {"scenario_1": ["CAF09661", "CAF09662"]},
                "scenario_name": "scenario_x1",
                "threshold": 1.5,  # Invalid threshold
            }
        ),
    )

    with pytest.raises(InvalidThresholdError) as exc_info:
        _load_mixed_scenarios_desc(spec_yaml)

    assert exc_info.value.threshold == 1.5


def test_check_mixed_scenarios_desc_invalid_overridden_ius_error(fs):
    spec_yaml = Path("mixed_scenarios_invalid_overridden_ius.yaml")
    fs.create_file(
        spec_yaml,
        contents=yaml.dump(
            {
                "disease": "lf",
                "default_scenario": "scenario_0",
                "overridden_ius": {},
                "scenario_name": "scenario_x1",
            }
        ),
    )

    with pytest.raises(InvalidOverriddenIUsError):
        _load_mixed_scenarios_desc(spec_yaml)


def test_check_mixed_scenarios_desc_invalid_overridden_ius_null(fs):
    spec_yaml = Path("mixed_scenarios_invalid_overridden_ius.yaml")
    fs.create_file(
        spec_yaml,
        contents=r"""
        disease: lf
        default_scenario: scenario_0
        overridden_ius: null
        scenario_name: scenario_x1
        """,
    )

    with pytest.raises(InvalidOverriddenIUsError):
        _load_mixed_scenarios_desc(spec_yaml)


def test_check_mixed_scenarios_desc_missing_fields_error(fs):
    spec_yaml = Path("mixed_scenarios_missing_fields.yaml")
    fs.create_file(
        spec_yaml,
        contents=yaml.dump(
            data={
                "disease": "lf",
                "default_scenario": "scenario_0",
                # Missing 'overridden_ius' and 'scenario_name'
            }
        ),
    )

    with pytest.raises(MissingFieldsError) as e:
        _load_mixed_scenarios_desc(spec_yaml)
    assert e.value.missing_fields == {"overridden_ius", "scenario_name"}


def test_check_mixed_scenarios_desc_valid(fs):
    spec_yaml_contents = {
        "disease": "lf",
        "default_scenario": "scenario_0",
        "overridden_ius": {
            "scenario_1": ["CAF09661", "CAF09662"],
            "scenario_2": ["CAF09663"],
        },
        "scenario_name": "scenario_x1",
    }
    fs.create_file(
        "mixed_scenarios_desc_valid.yaml",
        contents=yaml.dump(data=spec_yaml_contents),
    )
    try:
        desc = _load_mixed_scenarios_desc(Path("mixed_scenarios_desc_valid.yaml"))
    except Exception as e:
        pytest.fail(f"Test failed due to unexpected exception: {e}")
    else:
        assert desc.disease == spec_yaml_contents["disease"]
        assert desc.default_scenario == spec_yaml_contents["default_scenario"]
        assert desc.overridden_ius == spec_yaml_contents["overridden_ius"]
        assert desc.scenario_name == spec_yaml_contents["scenario_name"]
        assert desc.threshold is None


def test_load_mixed_scenarios_desc_file_not_found(fs):
    fs.create_file("some_file.yaml", contents="{}")

    non_existent_file_path = Path("non_existent_file.yaml")
    with pytest.raises(MixedScenariosFileNotFound) as e:
        _load_mixed_scenarios_desc(non_existent_file_path)

    assert e.value.file_path == non_existent_file_path


def test_check_mixed_scenarios_desc_duplicate_ius(fs):
    spec_yaml_contents = {
        "disease": "lf",
        "default_scenario": "scenario_0",
        "overridden_ius": {
            "scenario_1": ["CAF09661", "CAF09662"],
            "scenario_2": ["CAF09663", "CAF09661"],
        },
        "scenario_name": "scenario_x1",
    }
    fs.create_file(
        "mixed_scenarios_desc_duplicate_ius.yaml",
        contents=yaml.dump(data=spec_yaml_contents),
    )

    duplicate_ius_to_scenarios_mapping = {"CAF09661": {"scenario_1", "scenario_2"}}

    with pytest.raises(DuplicateIUError) as e:
        _load_mixed_scenarios_desc(Path("mixed_scenarios_desc_duplicate_ius.yaml"))

    assert e.value.ius_to_scenarios_mapping == duplicate_ius_to_scenarios_mapping


def test_check_mixed_scenarios_desc_missing_default_scenario(fs):
    input_scenarios = ["scenario_1", "scenario_2"]
    wd = Path("working_directory")
    input_directory = wd / "input"
    input_canonical_results = input_directory / "canonical_results"
    for scenario in input_scenarios:
        fs.create_dir(input_canonical_results / scenario)
    fs.create_file(input_directory / "PopulationMetadatafile.csv")

    spec_yaml = Path("mixed_scenarios_desc_missing_default_scenario.yaml")
    fs.create_file(
        spec_yaml,
        contents=yaml.dump(
            data={
                "disease": "lf",
                "default_scenario": "scenario_a",
                "overridden_ius": {
                    "scenario_1": ["CAF09661", "CAF09662"],
                    "scenario_2": ["CAF09663"],
                },
                "scenario_name": "scenario_x1",
            }
        ),
    )

    with pytest.raises(MissingScenariosFromSpecificationError) as e:
        _validate_working_directory(
            wd,
            _load_mixed_scenarios_desc(spec_yaml),
        )
    assert e.value.listed_scenarios == {"scenario_a"}


def test_check_mixed_scenarios_valid_working_directory(fs):
    input_scenarios = ["scenario_0", "scenario_1", "scenario_2"]
    wd = Path("working_directory")
    input_directory = wd / "input"
    input_canonical_results = input_directory / "canonical_results"
    for scenario in input_scenarios:
        fs.create_dir(input_canonical_results / scenario)
    fs.create_file(input_directory / "PopulationMetadatafile.csv")

    spec_yaml = Path("mixed_scenarios_desc.yaml")
    fs.create_file(
        spec_yaml,
        contents=yaml.dump(
            data={
                "disease": "lf",
                "default_scenario": "scenario_0",
                "overridden_ius": {
                    "scenario_1": ["CAF09661", "CAF09662"],
                    "scenario_2": ["CAF09663"],
                },
                "scenario_name": "scenario_x1",
            }
        ),
    )

    try:
        assert input_directory == _validate_working_directory(
            wd,
            _load_mixed_scenarios_desc(spec_yaml),
        )
    except Exception as e:
        pytest.fail(f"Test failed due to unexpected exception: {e}")


def test_check_mixed_scenarios_desc_missing_population_metadata_file(fs):
    input_scenarios = ["scenario_1", "scenario_2"]
    wd = Path("working_directory")
    input_directory = wd / "input"
    input_canonical_results = input_directory / "canonical_results"
    for scenario in input_scenarios:
        fs.create_dir(input_canonical_results / scenario)

    spec_yaml = Path("mixed_scenarios_desc.yaml")
    fs.create_file(
        spec_yaml,
        contents=yaml.dump(
            data={
                "disease": "lf",
                "default_scenario": "scenario_0",
                "overridden_ius": {
                    "scenario_1": ["CAF09661", "CAF09662"],
                    "scenario_2": ["CAF09663"],
                },
                "scenario_name": "scenario_x1",
            }
        ),
    )

    with pytest.raises(MissingPopulationMetadataFileError) as e:
        _validate_working_directory(
            wd,
            _load_mixed_scenarios_desc(spec_yaml),
        )
    assert e.value.path_to_file == input_directory / "PopulationMetadatafile.csv"


def test_check_mixed_scenarios_desc_missing_canonical_results_directory(fs):
    wd = Path("working_directory")
    input_directory = wd / "input"
    fs.create_dir(input_directory)
    fs.create_file(input_directory / "PopulationMetadatafile.csv")

    spec_yaml = Path("mixed_scenarios_desc.yaml")
    fs.create_file(
        spec_yaml,
        contents=yaml.dump(
            data={
                "disease": "lf",
                "default_scenario": "scenario_0",
                "overridden_ius": {
                    "scenario_1": ["CAF09661", "CAF09662"],
                    "scenario_2": ["CAF09663"],
                },
                "scenario_name": "scenario_x1",
            }
        ),
    )

    with pytest.raises(MissingCanonicalResultsDirectoryError) as e:
        _validate_working_directory(
            wd,
            _load_mixed_scenarios_desc(spec_yaml),
        )
    assert e.value.path_to_dir == input_directory / "canonical_results"


def test_check_mixed_scenarios_desc_missing_overridden_scenarios(fs):
    input_scenarios = ["scenario_1", "scenario_2"]
    wd = Path("working_directory")
    input_directory = wd / "input"
    input_canonical_results = input_directory / "canonical_results"
    for scenario in input_scenarios:
        fs.create_dir(input_canonical_results / scenario)
    fs.create_file(input_directory / "PopulationMetadatafile.csv")

    spec_yaml = Path("mixed_scenarios_desc_missing_default_scenario.yaml")
    fs.create_file(
        spec_yaml,
        contents=yaml.dump(
            data={
                "disease": "lf",
                "default_scenario": "scenario_0",
                "overridden_ius": {
                    "scenario_1x": ["CAF09661", "CAF09662"],
                    "scenario_2x": ["CAF09663"],
                },
                "scenario_name": "scenario_x1",
            }
        ),
    )

    with pytest.raises(MissingScenariosFromSpecificationError) as e:
        _validate_working_directory(
            wd,
            _load_mixed_scenarios_desc(spec_yaml),
        )
    assert e.value.listed_scenarios == {"scenario_0", "scenario_1x", "scenario_2x"}


def test_find_duplicate_ius_basic_duplicates():
    overridden_ius = {
        "scenario_1": ["IU_1", "IU_2", "IU_3", "IU_1"],
        "scenario_2": ["IU_2", "IU_4", "IU_5", "IU_2"],
        "scenario_3": ["IU_6", "IU_1", "IU_3", "IU_6"],
    }

    duplicate_iu_to_scenarios_mapping = {
        "IU_1": {"scenario_1", "scenario_3"},
        "IU_2": {"scenario_1", "scenario_2"},
        "IU_3": {"scenario_1", "scenario_3"},
        "IU_6": {"scenario_3"},
    }

    with pytest.raises(DuplicateIUError) as e:
        _find_duplicate_ius(overridden_ius)
    assert e.value.ius_to_scenarios_mapping == duplicate_iu_to_scenarios_mapping


def test_find_duplicate_ius_no_duplicates():
    overridden_ius = {
        "scenario_1": ["IU_1", "IU_2", "IU_3"],
        "scenario_2": ["IU_4", "IU_5", "IU_6"],
    }
    assert _find_duplicate_ius(overridden_ius) is None


def test_find_duplicate_ius_all_within_scenario():
    overridden_ius = {
        "scenario_1": ["IU_1", "IU_3", "IU_1"],
        "scenario_2": ["IU_2", "IU_4", "IU_5", "IU_2"],
    }

    duplicate_ius_to_scenarios_mapping = {"IU_1": {"scenario_1"}, "IU_2": {"scenario_2"}}
    with pytest.raises(DuplicateIUError) as e:
        _find_duplicate_ius(overridden_ius)
    assert e.value.ius_to_scenarios_mapping == duplicate_ius_to_scenarios_mapping


def test_find_duplicate_ius_empty_dict():
    assert _find_duplicate_ius({}) is None


def test_find_duplicate_ius_case_sensitivity():
    overridden_ius = {"scenario_1": ["IU_1", "iU_1"], "scenario_2": ["IU_2", "iU_2"]}

    assert _find_duplicate_ius(overridden_ius) is None


def test_find_duplicate_ius_complex_case():
    overridden_ius = {
        "scenario_1": ["IU_1", "IU_2", "IU_3", "IU_4", "IU_1", "IU_2"],
        "scenario_2": ["IU_3", "IU_5", "IU_6", "IU_3"],
        "scenario_3": ["IU_4", "IU_1", "IU_7", "IU_8", "IU_6", "IU_1"],
    }

    duplicate_ius_to_scenarios_mapping = {
        "IU_1": {"scenario_1", "scenario_3"},
        "IU_2": {"scenario_1"},
        "IU_3": {"scenario_1", "scenario_2"},
        "IU_4": {"scenario_1", "scenario_3"},
        "IU_6": {"scenario_2", "scenario_3"},
    }

    with pytest.raises(DuplicateIUError) as e:
        _find_duplicate_ius(overridden_ius)
    assert e.value.ius_to_scenarios_mapping == duplicate_ius_to_scenarios_mapping


def test_collect_source_target_paths(fs):
    # Setup fake directories and files
    input_canonical_results_dir = Path("/fake/input/canonical_results")
    output_canonical_results_dir = Path("/fake/output/canonical_results")
    output_scenario_directory = output_canonical_results_dir / "scenario_x1"

    fs.create_dir(input_canonical_results_dir)
    fs.create_dir(output_scenario_directory)

    # Prepare a MixedScenariosDescription mock object
    mixed_scenarios_desc = MixedScenariosDescription(
        disease="lf",
        scenario_name="scenario_x1",
        default_scenario="scenario_0",
        overridden_ius={
            "scenario_1": ["IU001", "IU002"],
            "scenario_2": ["IU003"],
        },
        threshold=None,
    )

    # Add fake input structure
    fs.create_dir(input_canonical_results_dir / mixed_scenarios_desc.default_scenario)
    fs.create_dir(input_canonical_results_dir / "scenario_1/IU0/IU001")
    fs.create_dir(input_canonical_results_dir / "scenario_1/IU0/IU002")
    fs.create_dir(input_canonical_results_dir / "scenario_2/IU0/IU003")

    # Call the function
    result = _collect_source_target_paths(
        input_canonical_results_dir,
        output_canonical_results_dir,
        mixed_scenarios_desc,
    )

    # Expected output
    expected = [
        (
            input_canonical_results_dir / mixed_scenarios_desc.default_scenario,
            output_scenario_directory,
        ),
        (
            input_canonical_results_dir / "scenario_1/IU0/IU001",
            output_scenario_directory / "IU0/IU001",
        ),
        (
            input_canonical_results_dir / "scenario_1/IU0/IU002",
            output_scenario_directory / "IU0/IU002",
        ),
        (
            input_canonical_results_dir / "scenario_2/IU0/IU003",
            output_scenario_directory / "IU0/IU003",
        ),
    ]

    # Assert that the result matches the expected output
    assert result == expected


def _create_json_schema_for_output_directory(
    input_directory: Path, mixed_scenarios_desc: MixedScenariosDescription
):
    input_canonical_results_dir = input_directory / "canonical_results"
    input_default_scenario_dir = input_canonical_results_dir / mixed_scenarios_desc.default_scenario

    def _process_country_directory(country_dir: Path):
        """Process a single country directory yielding (country_code, iu_info) pairs."""
        return (
            {
                "IU": iu_dir.name,
                "COUNTRY_CODE": iu_dir.name[:3],
                "IU_CODE": iu_dir.name[3:],  # extract the numeric portion
            }
            for iu_dir in country_dir.iterdir()
            if iu_dir.is_dir() and re.match(r"[A-Z]{3}\d{5}", iu_dir.name) is not None
        )

    ius_grouped_by_country = (
        (country_dir.name, _process_country_directory(country_dir))
        for country_dir in input_default_scenario_dir.iterdir()
        if country_dir.is_dir()
    )

    schema = {
        "type": "directory",
        "name": "output",
        "contents": [
            {
                "type": "directory",
                "name": "canonical_results",
                "contents": [
                    {
                        "type": "directory",
                        "name": mixed_scenarios_desc.scenario_name,
                        "contents": [],
                    }
                ],
            },
            {"type": "file", "name": "mixed_scenarios_metadata.json"},
        ],
    }

    output_canonical_results_dir = schema["contents"][0]  # canonical_results directory
    output_scenario_dir = output_canonical_results_dir["contents"][0]  # target scenario directory
    output_scenario_dir["contents"] = [
        {
            "type": "directory",
            "name": cc,
            "contents": [
                {
                    "type": "directory",
                    "name": iu["IU"],
                    "contents": [
                        {
                            "type": "file",
                            "name": f"{iu['IU']}_{mixed_scenarios_desc.scenario_name}_canonical.csv",
                        }
                    ],
                }
                for iu in ius
            ],
        }
        for cc, ius in ius_grouped_by_country
    ]

    return schema


def tree(directory: Path) -> dict:
    """
    Recursively generates a dictionary representing the directory and file structure.
    """
    output = {"type": "directory", "name": str(directory.name), "contents": []}
    try:
        # List all items in the directory
        for entry in directory.iterdir():
            if entry.is_dir():
                # If the entry is a directory, recursively call the function
                output["contents"].append(tree(entry))
            else:
                # If the entry is a file, add it to the contents
                output["contents"].append({"type": "file", "name": str(entry.name)})
    except PermissionError:
        # Handle permission errors gracefully
        output["contents"].append({"type": "error", "name": "Permission Denied"})

    return output


def test_mixed_scenarios_validate_output_directory(fs):
    wd = Path("disease")

    # Simulate a canonical set of input data for a disease
    input_directory = wd / "input"
    input_canonical_results = input_directory / "canonical_results"
    country_codes = ["AGO", "BFA", "CAF"]
    country_numeric_codes = ["09661", "09662", "09663"]
    for scenario in ["scenario_0", "scenario_1", "scenario_2"]:
        input_scenario_directory = input_canonical_results / scenario
        fs.create_dir(input_scenario_directory)
        for cc, nc in itertools.product(country_codes, country_numeric_codes):
            country_iu_code = f"{cc}{nc}"
            fs.create_file(
                input_scenario_directory
                / cc
                / country_iu_code
                / f"{country_iu_code}_{scenario}_canonical.csv"
            )

    fs.create_file(input_directory / "PopulationMetadatafile.csv")

    mixed_scenarios_desc = MixedScenariosDescription(
        disease="lf",
        default_scenario="scenario_0",
        overridden_ius={
            "scenario_1": ["CAF09661", "CAF09662"],
            "scenario_2": ["CAF09663"],
        },
        scenario_name="scenario_x1",
        threshold=None,
    )

    expected_output_json_schema = _create_json_schema_for_output_directory(
        input_directory, mixed_scenarios_desc
    )

    try:
        _validate_working_directory(wd, mixed_scenarios_desc)
    except Exception as e:
        pytest.fail(f"Test failed due to unexpected exception: {e}")

    ius_to_copy = _collect_source_target_paths(
        input_canonical_results, wd / "output" / "canonical_results", mixed_scenarios_desc
    )

    _prepare_output_directory(
        wd / "output", mixed_scenarios_desc, ius_to_copy, rename_target_scenario_column=False
    )

    output_directory_json_structure = tree(wd / "output")
    pprint.pprint(output_directory_json_structure, indent=2)

    assert output_directory_json_structure == expected_output_json_schema
