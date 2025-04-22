# Post-Processing with Mixed Scenarios

This script is used for post-processing results generated from mixed scenarios. It is essential that
the directory structure is properly set up for the script to function correctly. Please follow the
guidelines outlined below.

---

## Directory Structure

The working directory, can be named anything but probably makes sense to name it after a disease.
It should contain the following folders and files:

### Required Directory Structure:

```bash
disease_1/
├── mixed_scenarios_desc_1.yaml
├── mixed_scenarios_desc_2.yaml
├── mixed_scenarios_desc_3.yaml
├── input/
│   ├── canonical_results/
│   │   ├── scenario_0/
│   │   ├── scenario_1/
│   │   ├── scenario_2/
│   │   └── ... (additional scenarios as needed)
│   └── PopulationMetadatafile.csv
└── output/ (created by the script)
    ├── canonical_results/
    │   ├── scenario_x1/ (processed scenarios)
    │   └── ... (additional processed scenarios)
    ├── aggregated/
    ├── composite/
    ├── ius/
    ├── aggregation_info.json
    ├── iu_metadata.csv
    ├── mixed_scenarios_metadata.json
```

---

## Details

### User-Provided Files:

1. **Scenarios Description File `.yaml`**
    - This file contains the configuration for processing different scenarios. Its structure should
      follow the example provided below:
        ```yaml
           disease: lf                       # Valid are `lf`, `oncho` and `trachoma`
           threshold: 0.01                   # Optional - the threshold that counts as elimination 
                                             # (default 0.01 (1%), Trachoma should use 0.05 (5%))
           default_scenario: scenario_0      # Default scenario to process results from (optional)
           overridden_ius:
               scenario_1: [CAF09661, CAF09662]  # IUs to take from scenario_1
               scenario_2: [CAF09663]            # IUs to take from scenario_2
           scenario_name: scenario_x1        # Name applied to the processed scenario set
        ```


2. **`input`**
    - Must include the following:
        - **`canonical_results`**: Directory containing subfolders for each scenario, such as
          `scenario_0`, `scenario_1`, etc.
        - **`PopulationMetadatafile.csv`**: A CSV file with population metadata required for further
          processing.

### Created By Script:

1. **`output`**
    - This directory will be created automatically by the script. It contains the following:
        - **`canonical_results`**: Subfolders for processed scenarios, which are created based on
          the configuration in `mixed_scenarios_desc.yaml`.
        - **`mixed_scenarios_metadata.json`**: A JSON metadata file generated during the processing.

---

## Usage

To run the script, use the following command:

```bash
python post_process_mixed_scenarios.py -w path/to/working_directory -o path/to/output_directory -s path/to/yaml
```

---

## Notes

1. Ensure that all **user-provided files and directories** are correctly named and located as per
   the expected structure.
2. The `output_directory` does not need to exist beforehand; the script will create it if necessary.
3. The `path/to/yaml` and `path/to/output_directory` is *not* relative to the working directory, they can be located anywhere.

For any issues or questions, please refer to the comments in the script or reach out to the
development team.

## Optional default_scenario
There may be a case where you do not want to specify a default scenario. An example of this,
would be a scenario where you only want to mix-and-match IUs that are currently endemic,
and would want to exclude IUs that are not. In this case, you can list the IUs you want to
include in the aggregate within the specified scenario in the `overridden_ius` section, and leave
`default_scenario` blank. Only the IUs specified will be included.
```yaml
    disease: oncho                       # Valid are `lf`, `oncho` and `trachoma`
    threshold: 0.01                      # The threshold to compare to
    overridden_ius:
        scenario_1: [CAF09661, CAF09662] # IUs to take from scenario_1
        scenario_2: [CAF09663]           # IUs to take from scenario_2
    scenario_name: scenario_x1           # Name applied to the processed scenario set
```

## Using the Docker image

Install [Docker](https://docs.docker.com/get-started/get-docker/).

Build the image (this will take a few minutes)

```
docker build . -t mix-and-match
```

Run the command:

```
docker run --mount type=bind,src={absolute path to data},dst=/ntdmc/data mix-and-match -w /ntdmc/data -s /ntdmc/data/{scenario file}
```

Where `{absolute path to data}` should be replaced with path to the diseases working directory
(e.g. the path to the folder that contains the YAML file and the input folder)

And `{scenario file}` should be the name (only) of the mix YAML file (e.g. `mixed_scenarios_desc_1.yaml`).

So if you have LF downloaded to /home/username/ntdmc/lf/ then it would be:

```
docker run --mount type=bind,src=/home/username/ntdmc/lf/,dst=/ntdmc/data mix-and-match -w /ntdmc/data -s /ntdmc/data/mixed_scenarios_desc_1.yaml
```

_Note: the path cannot contain `~`. To find the full path for a folder use can use `pwd`._
