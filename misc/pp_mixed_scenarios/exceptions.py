from pathlib import Path
from typing import Iterable, Set, Dict


class MixedScenariosFileNotFound(FileNotFoundError):
    """Raised when the mixed scenarios description YAML file is not found."""

    def __init__(self, file_path: Path):
        self.file_path = file_path
        super().__init__(
            f"Required file '{self.file_path}' not found."
            f" Please ensure it exists in the working directory."
        )


class MissingFieldsError(ValueError):
    """Raised when the required fields in the mixed scenarios description YAML file are missing."""

    def __init__(self, missing_fields: Set[str]):
        self.missing_fields = missing_fields
        super().__init__(
            f"Invalid YAML structure. Missing required fields: {', '.join(self.missing_fields)}\n"
            f"Expected structure:\n"
            f"  default_scenario: <string>\n"
            f"  overridden_ius:\n"
            f"    <scenario>: [<IU1>, <IU2>, ...]\n"
            f"  scenario_name: <string>\n"
            f"  disease: <oncho|lf|trachoma>\n"
            f"  threshold: <number>"
        )


class InvalidOverriddenIUsError(ValueError):
    """Raised when the overridden IUs in the mixed scenarios description YAML file is not
    a dictionary."""

    def __init__(self):
        super().__init__(
            "The 'overridden_ius' field must be a dictionary where keys are scenarios and values"
            " are lists of IUs.\n"
            "Expected structure:\n"
            "  overridden_ius:\n"
            "    <scenario>: [<IU1>, <IU2>, ...]"
        )


class InvalidThresholdError(ValueError):
    """Raised when the 'threshold' field in the mixed scenarios description
    YAML file is invalid or out of range."""

    def __init__(self, threshold: float):
        self.threshold = threshold
        super().__init__(f"threshold is {self.threshold}, it must be between 0 and 1")


class InvalidDiseaseFieldError(ValueError):
    """Raised when the 'disease' field in the mixed scenarios description YAML
    file has an invalid or unrecognized value."""

    def __init__(self, invalid_disease: str, valid_set: Set[str]):
        self.disease = invalid_disease
        self.valid_set = valid_set
        super().__init__(
            f"Invalid 'disease' field - {self.disease}."
            f" Must be one of: {', '.join(self.valid_set)}."
        )


class DuplicateIUError(ValueError):
    """Raised when duplicate IUs are found among the overridden IUs."""

    def __init__(self, ius_to_scenarios_mapping: Dict[str, Iterable[str]]):
        self.ius_to_scenarios_mapping = ius_to_scenarios_mapping
        report = [
            f"{iu} was duplicated in {' and '.join(sorted(scenarios))}"
            for iu, scenarios in self.ius_to_scenarios_mapping.items()
        ]
        super().__init__(f"Duplicate IUs found in overridden_ius: {report}")


class InvalidInputDirectoryError(Exception):
    """Raised when there's a problem with the input directory."""

    def __init__(self, message: str):
        super().__init__(message)


class MissingPopulationMetadataFileError(InvalidInputDirectoryError):
    """Raised when the PopulationMetadatafile.csv is missing."""

    def __init__(self, path_to_file: Path):
        self.path_to_file = path_to_file
        super().__init__(
            f"Missing PopulationMetadatafile.csv in the input directory: {self.path_to_file}"
        )


class MissingCanonicalResultsDirectoryError(InvalidInputDirectoryError):
    """Raised when the 'canonical_results' directory is missing from the input directory."""

    def __init__(self, path_to_dir: Path):
        self.path_to_dir = path_to_dir
        super().__init__(
            f"Missing 'canonical_results' directory in the input directory: {self.path_to_dir}. "
            f"Ensure it contains the scenario directories containing the IUs"
        )


class MissingScenariosFromSpecificationError(InvalidInputDirectoryError):
    """Raised when scenarios mentioned in the YAML specification are missing from the
    input directory."""

    def __init__(self, listed_scenarios: Set[str]):
        self.listed_scenarios = listed_scenarios
        super().__init__(
            f"Scenarios mentioned in specification are missing from the input directory:"
            f" {', '.join(listed_scenarios)}"
        )
