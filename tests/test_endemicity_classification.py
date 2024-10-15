import pytest
from endgame_postprocessing.post_processing.endemicity_classification import (
    EndemicityClassifier,
)
import pandas as pd


def test_endemecity_classifier_is_endemic():
    classifier = EndemicityClassifier(
        endemic_states={"endemic"},
        non_endemic_states={"non-endemic"},
        missing_data_is_endemic=True,
    )
    assert classifier.is_state_endemic("endemic")


def test_endemecity_classifier_is_non_endemic():
    classifier = EndemicityClassifier(
        endemic_states={"endemic"},
        non_endemic_states={"non-endemic"},
        missing_data_is_endemic=True,
    )
    assert not classifier.is_state_endemic("non-endemic")


def test_endemecity_classifier_is_nan_is_non_endemic():
    classifier = EndemicityClassifier(
        endemic_states={"endemic"},
        non_endemic_states={"non-endemic"},
        missing_data_is_endemic=True,
    )
    assert classifier.is_state_endemic(pd.NA)


def test_endemecity_classifier_invalid_state():
    classifier = EndemicityClassifier(
        endemic_states={"endemic"},
        non_endemic_states={"non-endemic"},
        missing_data_is_endemic=True,
    )
    with pytest.raises(Exception) as e:
        classifier.is_state_endemic("random")
    assert e.match(
        r"Invalid endemic state: random - must be one of: ({'endemic', 'non-endemic'}|{'non-endemic', 'endemic'})"  # noqa: E501
    )
