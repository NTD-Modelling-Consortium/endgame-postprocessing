import sys
sys.path.append('../general-functions/')
from aggregation import aggregatePostProcessedFiles, iuLevelAggregate, countryLevelAggregate, africaLevelAggregate
from general_post_processing import processSingleFile
import os
import pandas as pd

lf_dir = "../../input-data/lf/"
scenarios = os.listdir(lf_dir)
scenario_index = 0
print(f'Total scenarios to process: {len(scenarios)}')
for lf_scenario in os.listdir(lf_dir):
    scenario_index += 1
    for country in os.listdir(os.path.join(lf_dir, lf_scenario)):
        if country.endswith("DS_Store"):
                continue
        for iu in os.listdir(os.path.join(lf_dir, lf_scenario, country)):
            if iu.endswith("DS_Store"):
                continue
            for output_file in os.listdir(os.path.join(lf_dir, lf_scenario, country, iu)):
                processSingleFile(
                    df=pd.read_csv(os.path.join(lf_dir, lf_scenario, country, iu, output_file)),
                    scenario=lf_scenario,
                    iuName=iu,
                    prevalence_marker_name="sampled mf prevalence (all pop)",
                    post_processing_start_time=2001,
                    measure_summary_map={"sampled mf prevalence (all pop)":None, "true mf prevalence (all pop)": None}
                ).to_csv("../../post-processed-outputs/lf/all_" + lf_scenario + "_" + iu + "_post_processed.csv")
    if ((len(scenarios) >= 10) and (scenario_index % (len(scenarios) // 10) == 0)):
        print(f"Scenarios Processed: {scenario_index} / {len(scenarios)}")
    

aggregated_df = aggregatePostProcessedFiles("../../post-processed-outputs/lf")
iuLevelAggregate(aggregated_df).to_csv("../../post-processed-outputs/aggregated/combined-lf-iu-lvl-agg.csv")
country_lvl_data = countryLevelAggregate(aggregated_df, 
                                         general_summary_cols=["sampled mf prevalence (all pop)", "year_of_1_mfp_avg", "year_of_90_under_1_mfp"],
                                         threshold_summary_cols = ["year_of_1_mfp_avg", "year_of_90_under_1_mfp"],
                                         )
country_lvl_data.to_csv("../../post-processed-outputs/aggregated/combined-lf-country-lvl-agg.csv")
africaLevelAggregate(
     country_lvl_data,
     measures_to_summarize=["sampled mf prevalence (all pop)"]
     ).to_csv("../../post-processed-outputs/aggregated/combined-lf-africa-lvl-agg.csv")
