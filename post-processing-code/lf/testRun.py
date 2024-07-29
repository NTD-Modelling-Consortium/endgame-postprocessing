import sys
sys.path.append('../general-functions/')
from aggregation import aggregateAndCalculate
from general_post_processing import processSingleFile
import os

lf_dir = "../../input-data/lf/"
for lf_scenario in os.listdir(lf_dir):
    for country in os.listdir(os.path.join(lf_dir, lf_scenario)):
        for iu in os.listdir(os.path.join(lf_dir, lf_scenario, country)):
            for output_file in os.listdir(os.path.join(lf_dir, lf_scenario, country, iu)):
                processSingleFile(
                    os.path.join(lf_dir, lf_scenario, country, iu, output_file),
                    lf_scenario,
                    iu,
                    "../../post-processed-outputs/lf/",
                    prevalence_marker_name="sampled mf prevalence (all pop)",
                    post_processing_start_time=2001,
                    model="LF",
                )
    
aggregateAndCalculate("../../post-processed-outputs/lf", output_file_root="../../post-processed-outputs/aggregated/combined-lf")
