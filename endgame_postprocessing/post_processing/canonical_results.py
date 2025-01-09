from pandas import DataFrame
from endgame_postprocessing.post_processing.custom_file_info import CustomFileInfo



CanonicalResults = dict[str, dict[str, tuple[CustomFileInfo, DataFrame]]]
'''
A dictionary that maps from scenario -> iu -> (file_info, data)

The data should be the canonical form of the data referenced by the custom file info
that is for the IU and scenario that it is indexed by
'''
