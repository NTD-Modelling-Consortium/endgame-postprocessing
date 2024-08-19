MEASURE_COLUMN_NAME = "measure"

# Country Level Params
COUNTRY_SUMMARY_COLUMNS = [
    "prevalence",
    "year_of_threshold_prevalence_avg",
    "year_of_pct_runs_under_threshold",
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
    "year_of_pct_runs_under_threshold",
]
COUNTRY_THRESHOLD_SUMMARY_GROUP_COLUMNS = ["scenario", "country_code", "measure"]
COUNTRY_THRESHOLD_RENAME_MAP = {
    "year_of_pct_runs_under_threshold": "pct_of_ius_passing_90pct_threshold",
    "year_of_threshold_prevalence_avg": "pct_of_ius_passing_avg_threshold",
}

# Africa Level Params
AFRICA_SUMMARY_MEASURES = ["prevalence"]
AFRICA_LVL_GROUP_COLUMNS = ["scenario", "year_id", "age_start", "age_end", "measure"]

# Disease Specifics
ONCHO_MEASURES = ["APOD", "Atrophy", "Blindness", "CPOD", "Depigmentation", "HangingGroin",
                  "OAE_prevalence", "RSDComplex", "RSDSimple", "SevereItching", "VisualImpairment",
                  "intensity", "mean_worm_burden", "number", "pnc", "prevalence"
]
