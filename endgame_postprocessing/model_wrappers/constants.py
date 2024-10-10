MEASURE_COLUMN_NAME = "measure"

PCT_RUNS_UNDER_THRESHOLD = [0.50, 0.75, 0.85, 0.90, 0.95]

# Country Level Params
COUNTRY_SUMMARY_COLUMNS = [
    f"year_of_{int(pct * 100)}pct_runs_under_threshold"
    for pct in PCT_RUNS_UNDER_THRESHOLD
]

COUNTRY_SUMMARY_GROUP_COLUMNS = [
    "scenario_name",
    "country_code",
    "year_id",
    "age_start",
    "age_end",
    "measure",
]

COUNTRY_THRESHOLD_SUMMARY_COLUMNS = [
    f"year_of_{int(pct * 100)}pct_runs_under_threshold"
    for pct in PCT_RUNS_UNDER_THRESHOLD
]

COUNTRY_THRESHOLD_SUMMARY_GROUP_COLUMNS = ["scenario", "country_code", "measure"]

COUNTRY_THRESHOLD_RENAME_MAP = {
    f"year_of_{int(pct * 100)}pct_runs_under_threshold":
    f"ius_passing_{int(pct * 100)}pct_under_threshold"
    for pct in PCT_RUNS_UNDER_THRESHOLD
}

# Africa Level Params
AFRICA_SUMMARY_MEASURES = ["processed_prevalence"]
AFRICA_LVL_GROUP_COLUMNS = ["scenario", "year_id", "age_start", "age_end", "measure"]

# Disease Specifics
ONCHO_MEASURES = ["APOD", "Atrophy", "Blindness", "CPOD", "Depigmentation", "HangingGroin",
                  "OAE_prevalence", "RSDComplex", "RSDSimple", "SevereItching", "VisualImpairment",
                  "intensity", "mean_worm_burden", "number", "pnc", "prevalence"]
