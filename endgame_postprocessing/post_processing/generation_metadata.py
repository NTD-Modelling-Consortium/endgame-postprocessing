import os
import warnings

import endgame_postprocessing


def _warning_to_dictionary(generated_warnings):
    root = os.path.dirname(endgame_postprocessing.__file__)
    return [{
        "message": str(warning.message),
        "file": os.path.relpath(warning.filename, root),
        "line": warning.lineno
    } for warning in generated_warnings]

def produce_generation_metadata(*,
                                warnings: list[warnings.WarningMessage]):
    return {
        "warnings": _warning_to_dictionary(warnings)
    }
