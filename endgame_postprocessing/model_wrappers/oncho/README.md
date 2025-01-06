# Usage

Having installed the endgame post processing pipeline, there exists functions for running 
the Oncho pipeline. 

It can concatenate historic data and forward projections.

```python
import endgame_postprocessing.model_wrappers.oncho.testRun as oncho_runner

oncho_runner.run_postprocessing_pipeline(
        input_dir=input_data, output_dir=output_path, historic_dir=historic_data
    )
```

Here the `input_dir` is the directory of the forward projections. 
This is expected to be in the following format:

 - scenario_1/
   - AAA/
     - AAA00001/
       - ntdmc-AAA00001-oncho-scenario_1-200.csv
     - .../
   - .../
 - scenario_2/
   - AAA/
     - .../
   - .../
 - ...

 The `output_dir` is the directory the generated files will be put. This will match the 
 structure detailed in main [README](../../../README.md#directory-structure).

 The `historic_data` is the directory containing the pre-2026 historic data that will 
 be prepended before each forward projection. 
 This can be set to `None` if no historic data to concatenate.

 **ATTENTION: The structure and file names for this are different - it is expected to be just flat.**

 That is, it is expected to be:

  - output_full_MTP_AAAXXXX00001.csv
  - output_full_MTP_AAAXXXX00002.csv
  - ...
