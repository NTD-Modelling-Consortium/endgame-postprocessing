# Usage

Having installed the aggregates pipeline (this repo), there exists functions
for running the LF pipeline.

```
from endgame_postprocessing.model_wrappers.lf import testRun

testRun.run_postprocessing_pipeline(
    forward_projection_raw,
    historic_data_nonstandard,
    output_path
)
```

This will aggregates into standard format the input files found in `forward_projection_raw`.
Appended to with historic data found in `historic_data_nonstandard`

`forward_projections_path` should contain files (in any structure with names matching):
    - `ntdmc-AAA12345-lf-scenario_0-200.csv` (for an IU AAA12345 and scenario 0)
    - `PopulationMetadatafile.csv` (must be at the root)

`historic_data_nonstandard` should be a path to the files Matt generated for historic data
which contain just a year column and then draw_0,... where the values are the
protocol prevalence.

The output directory must be empty.
On completion the sub-structure will be:
output_dir/
    ius/
        a csv per IU with name format
        scenario1_iu1_post_processed.csv
    aggregated/
        combined-lf-iu-lvl-agg.csv - all IUs in one csv
            a aggregated by country csv
        combined-lf-country-lvl-agg.csv - aggregate by country
        combined-lf-africa-lvl-agg.csv - aggregated across Africa