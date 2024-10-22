# Usage

Having installed the endgame post processing pipeline, there exists functions for running 
both the STH and SCH pipelines.

## STH

The STH pipeline can either create a "any worm" result, or a single worm result. 

### Any Worm

```python
import endgame_postprocessing.model_wrappers.sch.run_sch as run_sch

input_dir = # TODO
# Collect all of the worm directories that are in the input_dir
worm_directories = next(os.walk(input_dir))[1]
run_sch.run_sth_postprocessing_pipeline(
    input_dir,
    "desired output directory",
    worm_directories,
    1,
    skip_canonical=False,
)
```

### Single Worm

Alternatively, can be run for each worm separately:

```python
import endgame_postprocessing.model_wrappers.sch.run_sch as run_sch

input_dir = # TODO
worm_directories = next(os.walk(input_dir))[1]
for worm_directory in worm_directories:
    run_sch.run_sth_postprocessing_pipeline(
        input_dir,
        f"local_data/sth-output-single-worm/{worm_directory}",
        [worm_directory],
        1,
        skip_canonical=False,
    )
```

## skip_canonical

In all cases, there is an optional parameter `skip_canonical`, if this is
set to `True` then the canonical step will be skipped. 

This step is required, if you skip it you will get the following error:

```
FileNotFoundError: [Errno 2] No such file or directory: './output/canonical_results/'
```

However, if it completes successfully and you want to re-run the aggregation, then this can be used.

For example, if you forget to include the `PopulationMetaDatafile.csv` on the first run, then this can skip to that bit of the pipeline. 
