PERCENTILES_TO_CALC = [2.5, 5, 10, 25, 50, 75, 90, 95, 97.5]
MEASURE_COLUMN_NAME = "measure"

# Country Level Params
COUNTRY_SUMMARY_COLUMNS = [
    "prevalence",
    "year_of_threshold_prevalence_avg",
    "year_of_90_under_threshold",
]
COUNTRY_SUMMARY_GROUP_COLUMNS = [
    "scenario",
    "country_code",
    "year_id",
    "age_start",
    "age_end",
    "measure",
]
COUNTRY_THRESHOLD_SUMMARY_COLUMNS = [
    "year_of_threshold_prevalence_avg",
    "year_of_90_under_threshold",
]
COUNTRY_THRESHOLD_SUMMARY_GROUP_COLUMNS = ["scenario", "country_code", "measure"]
COUNTRY_THRESHOLD_RENAME_MAP = {
    "year_of_90_under_threshold": "pct_of_ius_passing_90pct_threshold",
    "year_of_threshold_prevalence_avg": "pct_of_ius_passing_avg_threshold",
}

# Africa Level Params
AFRICA_SUMMARY_MEASURES = ["prevalence"]
AFRICA_LVL_GROUP_COLUMNS = ["scenario", "year_id", "age_start", "age_end", "measure"]
