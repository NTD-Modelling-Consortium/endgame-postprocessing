import sys
sys.path.append('../general-functions/')
from aggregation import aggregateAndCalculate
from general_post_processing import processSingleFile
import os

oncho_dir = "../../input-data/oncho/"
for oncho_scenario in os.listdir(oncho_dir):
    for country in os.listdir(os.path.join(oncho_dir, oncho_scenario)):
        for iu in os.listdir(os.path.join(oncho_dir, oncho_scenario, country)):
            for output_file in os.listdir(os.path.join(oncho_dir, oncho_scenario, country, iu)):
                if (not(output_file.endswith("raw_all_age_data.csv"))): 
                    continue            
                processSingleFile(
                    os.path.join(oncho_dir, oncho_scenario, country, iu, output_file),
                    oncho_scenario,
                    iu,
                    "../../post-processed-outputs/oncho/",
                    prevalence_marker_name="prevalence",
                    post_processing_start_time=2026,
                    model="ONCHO",
                )
    
aggregateAndCalculate("../../post-processed-outputs/oncho", output_file_root="../../post-processed-outputs/aggregated/combined-oncho")
