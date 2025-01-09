import warnings
from endgame_postprocessing.post_processing.warnings_collector import CollectAndPrintWarnings, warning_to_dictionary

class PrintCollection:
    def __init__(self):
        self.writes = []
    def write(self, s):
        self.writes.append(s)

def test_warning_collector_collects_warnings():
    with CollectAndPrintWarnings(output=None) as raised_warnings:
        warnings.warn("Hello")

    assert [str(warning.message) for warning in raised_warnings] == ["Hello"]
    assert [warning.lineno for warning in raised_warnings] == [12]

def test_warning_collector_still_prints_warning():
    output = PrintCollection()
    with CollectAndPrintWarnings(output=output):
        warnings.warn("Hello")

    assert len(output.writes) == 2
    assert output.writes[0] == "Warning: Hello (test_warnings_collector.py:20)"
    assert output.writes[1] == "\n"

def test_outer_warning_collector_restored():
    with warnings.catch_warnings(record=True) as outer_warnings:
        with CollectAndPrintWarnings(output=None) as inner_warnings:
            warnings.warn("Inner")
        warnings.warn("Outer")

    assert [str(warning.message) for warning in outer_warnings] == ["Outer"]
    assert [str(warning.message) for warning in inner_warnings] == ["Inner"]

def test_build_dictionary():
    message = warnings.WarningMessage("My warning", Warning, __file__, 12)
    assert warning_to_dictionary(message) == {
        "message": "My warning",
        "file": "../tests/post_processing/test_warnings_collector.py",
        "line": 12
    }
