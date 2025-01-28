# Usage

Having installed the endgame post processing pipeline, there exists functions for running
the Trachoma pipeline.

It can concatenate historic data and forward projections.

```python
from endgame_postprocessing.model_wrappers.trachoma import run_trach

run_trach.run_postprocessing_pipeline(
    input_dir=input_dir,
    output_dir=output_path,
    historic_dir=historic_dir,
    historic_prefix="PrevDataset_Trachoma_",
    start_year=2000,
    stop_year=2041
)
```

Here the `input_dir` is the directory (with any structure) of the forward projections *and* the
Population file -

- ntdmc-AAA00001-trachoma-scenario_1-200.csv (for an IU AAA00001 and scenario 1)
- ntdmc-AAA00002-trachoma-scenario_1-200.csv (for an IU AAA00002 and scenario 1)
- PopulationMetadatafile.csv
- ...

The `output_dir` is the directory the generated files will be put. This will match the
structure detailed in main [README](../../../README.md#directory-structure).

The `historic_dir` is the directory containing the historic data that will
be prepended before each forward projection.
This can be set to `None` if there's no historic data to concatenate and this step will be skipped.

The `historic_prefix` is the prefix of the file names containing the historic data.
This is defaulted to "*", but can be configured to a specific prefix if multiple types of files
are within the directory. The prefix must include all characters up to the start of the country code.
I.e: if the file name is `PrevDataset_Trachoma_AAA00001.csv`, the historic_prefix value would be
`"PrevDataset_Trachoma_"`. The prefix can also include regex - i.e `"*PrevDataset_Trachoma*"` or
`"PrevDataset_Trachoma_*"`,
etc.

The `start_year` is the first year that will be included in the results.

The `stop_year` is the last year that will be included in the results
(i.e. is inclusive of this year)