import os
import sys
import warnings

import endgame_postprocessing


class CollectAndPrintWarnings:
    '''
    A re-implementation of warnings.catch_warnings but that
    still prints out the warnings
    '''
    def __init__(self, output=sys.stderr):
        self.warnings = []
        self.output = output

    def __enter__(self):
        self.old_warning_method = warnings.showwarning
        warnings.showwarning = self._show_warning
        self.warnings = []
        return self.warnings

    def __exit__(self, *exc_info):
        warnings.showwarning = self.old_warning_method

    def _show_warning(self, message, category, filename, lineno, file=None, line=None):
        warning_message = warnings.WarningMessage(message, category, filename, lineno, file, line)
        self.warnings.append(warning_message)
        short_file_name = os.path.basename(filename)
        print(f"Warning: {message} ({short_file_name}:{lineno})", file=self.output)

def warning_to_dictionary(warning: warnings.WarningMessage):
    root = os.path.dirname(endgame_postprocessing.__file__)
    return {
        "message": str(warning.message),
        "file": os.path.relpath(warning.filename, root),
        "line": warning.lineno
    }
