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

## Schisto

The SCH pipeline can also create an "any worm" result, or a single worm result. 

### Any Worm

Unlike STH, the SCH worm structure is differeny by worm. Haematobium is simulated for for all IUs, however mansoni is split by high or low burden. As such, we need to explicitly state the folder order for the pipeline to properly process the data. 

In this case, the "any worm" functionality will expect that an IU that is in haematobium will appear once in either `mansoni-high-burden` or `mansoni-low-burden`. 

```python
import endgame_postprocessing.model_wrappers.sch.run_sch as run_sch

root_input_dir = # TODO
worm_directories = ["sch-haematobium", "sch-mansoni-high-burden", "sch-mansoni-low-burden"]
run_sch_postprocessing_pipeline(
  f"{root_input_dir}/",
  "local_data/sch-output-all-worm/",
  skip_canonical=False,
  worm_directories=worm_directories,
)
```

### Single Worm

```python
import endgame_postprocessing.model_wrappers.sch.run_sch as run_sch

root_input_dir = # TODO
worm_directories = next(os.walk(root_input_dir))[1]
for worm_directory in worm_directories:
    run_sch.run_sch_postprocessing_pipeline(
        f"{root_input_dir}/",
        f"local_data/sch-output-single-worm/{worm_directory}",
        skip_canonical=False,
        worm_directories=[worm_directory]
    )
```

It expects files inside this directory (arbitrarily nested) matching the following regex:

```
ntdmc-(?P<iu_id>(?P<country>[A-Z]{3})\d{5})-[\w_]+-group_001-(?P<scenario>scenario_\w+)-survey_type_kk2-group_001-200_simulations.csv
```

Additionally, each worm directory needs to have a copy of the `PopulationMetadatafile.csv`.
That is, the root_input_dir should look like:

root_input_dir/ 
 - sch-haematobium/
   - PopulationMetadatafile.csv
   - **/ntdmc-*.csv
 - sch-mansoni-high-burden/
   - PopulationMetadatafile.csv
   - **/ntdmc-*.csv
 - sch-mansoni-low-burden/
   - PopulationMetadatafile.csv
   - **/ntdmc-*.csv

## skip_canonical

In all cases, there is an optional parameter `skip_canonical`, if this is
set to `True` then the canonical step will be skipped. 

This step is required, if you skip it you will get the following error:

```
FileNotFoundError: [Errno 2] No such file or directory: './output/canonical_results/'
```

However, if it completes successfully and you want to re-run the aggregation, then this can be used.

For example, if you forget to include the `PopulationMetaDatafile.csv` on the first run, then this can skip to that bit of the pipeline. 
