from endgame_postprocessing.post_processing import (
    canonicalise,
    output_directory_structure,
    pipeline,
)
from endgame_postprocessing.post_processing.disease import Disease
from endgame_postprocessing.post_processing.file_util import (
    post_process_file_generator,
)
from tqdm import tqdm
import pandas as pd


def canonicalise_raw_oncho_results(input_dir, output_dir, start_year=1970, stop_year=2041):
    file_iter = post_process_file_generator(
        file_directory=input_dir, end_of_file="-raw_all_age_data.csv"
    )

    all_files = list(file_iter)

    for file_info in tqdm(all_files, desc="Canoncialise oncho results"):
        raw_iu = pd.read_csv(file_info.file_path)
        # TODO: canonical shouldn't need the age_start / age_end but these are assumed present later
        raw_iu = raw_iu[(raw_iu['year_id'] >= start_year) & (raw_iu['year_id'] <= stop_year)]
        canonical_result = canonicalise.canonicalise_raw(
            raw_iu, file_info, "prevalence"
        )
        output_directory_structure.write_canonical(
            output_dir, file_info, canonical_result
        )


oncho_dir = "local_data/oncho"
working_directory = "local_data/output"

canonicalise_raw_oncho_results(oncho_dir, working_directory, 1970, 2041)
pipeline.pipeline(oncho_dir, working_directory, disease=Disease.ONCHO)
