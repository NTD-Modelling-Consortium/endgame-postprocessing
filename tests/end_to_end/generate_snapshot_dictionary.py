import os
from pathlib import Path


def generate_snapshot_dictionary(path):
    """
    Returns a dictionary whose keys are the files and folders found at
    path, and values are either the contents of the file, or a dictionary like
    this one for the directory specified.

    These can then be used as a comparison against a snapshot
    produced by pytest-snapshot
    """
    results = {}
    path_to_root, subdirs, files = next(os.walk(path))

    root_path = Path(path_to_root)
    for file in files:
        results[file] = (root_path / Path(file)).read_text()

    for subdir in subdirs:
        results[subdir] = generate_snapshot_dictionary(root_path / Path(subdir))

    return results
