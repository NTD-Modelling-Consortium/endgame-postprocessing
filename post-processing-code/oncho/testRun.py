import sys
sys.path.append('../general-functions/')
from aggregation import aggregatePostProcessedFiles, iuLevelAggregate, countryLevelAggregate, africaLevelAggregate
from general_post_processing import processSingleFile
import os
import pandas as pd

oncho_dir = "../../input-data/oncho/"
scenarios = os.listdir(oncho_dir)
scenario_index = 0
print(f'Total scenarios to process: {len(scenarios)}')
for oncho_scenario in scenarios:
    scenario_index += 1
    for country in os.listdir(os.path.join(oncho_dir, oncho_scenario)):
        if country.endswith("DS_Store"):
                continue
        for iu in os.listdir(os.path.join(oncho_dir, oncho_scenario, country)):
            if iu.endswith("DS_Store"):
                continue
            for output_file in os.listdir(os.path.join(oncho_dir, oncho_scenario, country, iu)):
                if (not(output_file.endswith("raw_all_age_data.csv"))): 
                    continue     
                processSingleFile(
                    df=pd.read_csv(os.path.join(oncho_dir, oncho_scenario, country, iu, output_file)),
                    scenario=oncho_scenario,
                    iuName=iu,
                    prevalence_marker_name="prevalence",
                    post_processing_start_time=1970,
                    measure_summary_map={"all": "default"}
                ).to_csv("../../post-processed-outputs/oncho/" + oncho_scenario + "_" + iu + "post_processed.csv")
    if ((len(scenarios) >= 10) and (scenario_index % (len(scenarios) // 10) == 0)):
        print(f"Scenarios Processed: {scenario_index} / {len(scenarios)}")

aggregated_df = aggregatePostProcessedFiles("../../post-processed-outputs/oncho")
iuLevelAggregate(aggregated_df).to_csv("../../post-processed-outputs/aggregated/combined-oncho-iu-lvl-agg.csv")
country_lvl_data = countryLevelAggregate(aggregated_df)
country_lvl_data.to_csv("../../post-processed-outputs/aggregated/combined-oncho-country-lvl-agg.csv")
africaLevelAggregate(country_lvl_data).to_csv("../../post-processed-outputs/aggregated/combined-oncho-africa-lvl-agg.csv")
