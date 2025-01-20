import warnings

from endgame_postprocessing.post_processing.generation_metadata import produce_generation_metadata


def test_produce_generation_metadata():
    message = warnings.WarningMessage("My warning", Warning, __file__, 12)
    assert produce_generation_metadata(warnings=[message]) == {
        "warnings": [
            {
                "message": "My warning",
                "file": "../tests/post_processing/test_generation_data.py",
                "line": 12
            }
        ]
    }
