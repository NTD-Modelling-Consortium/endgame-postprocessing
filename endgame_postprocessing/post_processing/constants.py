YEAR_COLUMN_NAME = "year_id"
MEASURE_COLUMN_NAME = "measure"
AGE_START_COLUMN_NAME = "age_start"
AGE_END_COLUMN_NAME = "age_end"
DRAW_COLUMNN_NAME_START = "draw_"


PERCENTILES_TO_CALC = [2.5, 5, 10, 25, 50, 75, 90, 95, 97.5]

AGGEGATE_DEFAULT_TYPING_MAP = {
    "year_id": float,
    "age_start": float,
    "age_end": float,
    "mean": float,
    "2.5_percentile": float,
    "5_percentile": float,
    "10_percentile": float,
    "25_percentile": float,
    "50_percentile": float,
    "75_percentile": float,
    "90_percentile": float,
    "95_percentile": float,
    "97.5_percentile": float,
    "median": float,
    "standard_deviation": float,
}

FINAL_COLUMNS = (
    [
        "iu_name",
        "country_code",
        "scenario",
        "year_id",
        "age_start",
        "age_end",
        "measure",
        "mean",
    ]
    + [str(p) + "_percentile" for p in PERCENTILES_TO_CALC]
    + ["standard_deviation", "median"]
)
