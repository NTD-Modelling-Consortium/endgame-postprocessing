# Delta Years Computation Script

This directory contains a standalone utility script for computing delta years between scenarios for disease elimination modeling.

## Overview

The `compute_delta_years.py` script analyzes canonical IU (Implementation Unit) data to compute the difference in years between when each scenario reaches an elimination threshold compared to a reference scenario. This is useful for comparing intervention strategies and understanding their relative timing for achieving elimination goals.

### What are Delta Years?

Delta years represent the year difference between when a scenario reaches the prevalence elimination threshold compared to a reference scenario. The computation produces two types of measures to handle cases where scenarios may not reach the threshold within the time range:

**For standard comparisons (both scenarios reach threshold):**
- **Positive values**: Scenario takes longer to reach elimination than the reference
- **Negative values**: Scenario reaches elimination faster than the reference
- **Zero values**: Scenario reaches elimination in the same year as the reference

**For scenarios that don't reach threshold (atleast_delta_years measures):**
- **Positive values**: Scenario takes at least this many years longer than the reference
- **Negative values**: Scenario reaches elimination at least this many years faster than the reference
- **999 values**: Neither scenario reaches the threshold, making comparison impossible

## Usage

### Basic Usage

```bash
# Compute delta years with default settings (threshold=0.01, auto-detect reference scenario)
python misc/compute_delta_years.py /path/to/canonical_results/
```

### Advanced Usage

```bash
# Custom threshold and output file
python misc/compute_delta_years.py /path/to/canonical_results/ \
    --threshold 0.005 \
    --output custom_delta_years.csv

# Specify reference scenario explicitly
python misc/compute_delta_years.py /path/to/canonical_results/ \
    --reference-scenario scenario_1

# Include only specific scenarios
python misc/compute_delta_years.py /path/to/canonical_results/ \
    --scenarios scenario_1 scenario_2 scenario_3a

# Verbose output for debugging
python misc/compute_delta_years.py /path/to/canonical_results/ \
    --verbose
```

## Command Line Arguments

| Argument | Required | Default | Description |
|----------|----------|---------|-------------|
| `canonical_dir` | ✅ | - | Path to directory containing canonical results |
| `--threshold` | ❌ | 0.01 | Prevalence threshold for elimination |
| `--reference-scenario` | ❌ | auto-detect | Name of reference scenario to compare against |
| `--prevalence-measure` | ❌ | processed_prevalence | Name of prevalence measure column |
| `--output` | ❌ | delta_years_output.csv | Output CSV file name |
| `--scenarios` | ❌ | all found | Specific scenarios to include |
| `--verbose` | ❌ | false | Print detailed progress information |

## Input Requirements

### Directory Structure

The script expects canonical results in the standard endgame post-processing directory structure:

```
canonical_results/
├── scenario_1/
│   ├── AAA/
│   │   ├── AAA00001/
│   │   │   └── scenario_1_AAA00001_canonical.csv
│   │   └── AAA00002/
│   │       └── scenario_1_AAA00002_canonical.csv
│   └── BBB/
│       └── BBB00003/
│           └── scenario_1_BBB00003_canonical.csv
└── scenario_2/
    ├── AAA/
    │   ├── AAA00001/
    │   │   └── scenario_2_AAA00001_canonical.csv
    │   └── AAA00002/
    │       └── scenario_2_AAA00002_canonical.csv
    └── BBB/
        └── BBB00003/
            └── scenario_2_BBB00003_canonical.csv
```

### File Format

Each canonical CSV file must contain:
- **Required columns**: scenario, iu_name, country_code, measure, year_id
- **Draw columns**: draw_0, draw_1, ..., draw_N (simulation draws)
- **Prevalence data**: Rows where measure equals the prevalence measure name

## Output Format

The output CSV contains the following columns:

| Column | Description |
|--------|-------------|
| `iu_name` | Implementation Unit identifier |
| `country_code` | Country code for the IU |
| `scenario` | Scenario name |
| `measure` | Either "delta_years_{reference_scenario}" or "atleast_delta_years_{reference_scenario}" |
| `draw_0, draw_1, ..., draw_N` | Delta years for each simulation draw |

### Sample Output

```csv
iu_name,country_code,scenario,measure,draw_0,draw_1,draw_2,...
AAA00001,AAA,scenario_1,delta_years_scenario_1,0,0,0,...
AAA00001,AAA,scenario_2,delta_years_scenario_1,2,1,3,...
AAA00002,AAA,scenario_1,atleast_delta_years_scenario_1,0,0,0,...
AAA00002,AAA,scenario_2,atleast_delta_years_scenario_1,999,5,2,...
```

**Note**: The measure name indicates the reliability of the comparison:
- `delta_years_*`: Both scenarios reach the threshold within the time range
- `atleast_delta_years_*`: At least one scenario doesn't reach the threshold (values represent minimum bounds)

## Example Scripts

The `examples/` directory contains ready-to-use shell scripts for common analysis patterns:

- **`oncho_example.sh`**: Oncho analysis with 1% threshold
- **`lf_example.sh`**: LF analysis with auto-detection
- **`trachoma_example.sh`**: Trachoma analysis with 5% threshold  
- **`custom_analysis_example.sh`**: Advanced usage with custom parameters

To use an example script:
```bash
# Edit the script to set your data paths
nano misc/compute_delta_years/examples/oncho_example.sh

# Run the script
bash misc/compute_delta_years/examples/oncho_example.sh
```

## Detailed Examples

### Example 1: Basic Oncho Analysis

```bash
# Compute delta years for oncho data with standard 1% threshold
python misc/compute_delta_years.py tests/end_to_end/oncho/generated_data/canonical_results/
```

Output summary:
```
Loading canonical IU files from: tests/end_to_end/oncho/generated_data/canonical_results/
Loading files: 100%|████████| 8/8 [00:00<00:00, 45.2file/s]
Successfully loaded 8 canonical IU files
  Files processed: 8
  Files loaded: 8
  Files skipped: 0

Data Summary:
  Scenarios found: ['scenario_1', 'scenario_2']
  Total canonical IU files: 8
    scenario_1: 4 IUs
    scenario_2: 4 IUs

Computing delta years...
  Threshold: 0.01
  Prevalence measure: processed_prevalence
  Reference scenario: auto-detect

✅ Delta years computation completed successfully!
   Output saved to: delta_years_output.csv
   Rows: 8
   Columns: 204
   Reference scenario used: scenario_1
   Scenarios in results: ['scenario_1', 'scenario_2']
```

### Example 2: Custom Analysis with Specific Parameters

```bash
# Analyze only specific scenarios with custom threshold
python misc/compute_delta_years.py /path/to/data/ \
    --scenarios scenario_1 scenario_3a \
    --threshold 0.005 \
    --reference-scenario scenario_1 \
    --output elimination_comparison.csv \
    --verbose
```

### Example 3: LF with Different Threshold

```bash
# LF analysis with 0.5% threshold
python misc/compute_delta_years.py tests/end_to_end/lf/data_no_historic/generated_data/canonical_results/ \
    --threshold 0.005 \
    --output lf_delta_years_0.5pct.csv
```

## Troubleshooting

### Common Issues

1. **No canonical files found**
   - Check that the directory contains files ending with `_canonical.csv`
   - Verify the directory path is correct

2. **Missing required columns**
   - Ensure canonical files have been generated properly through the pipeline
   - Check that files contain scenario, iu_name, country_code, measure, year_id columns

3. **No draw columns**
   - Verify that the canonical files contain simulation draw data (draw_0, draw_1, etc.)

4. **Reference scenario not found**
   - Check scenario names in your data
   - Use `--scenarios` to list available scenarios first
   - Specify `--reference-scenario` explicitly

### Verbose Output

Use the `--verbose` flag to see detailed information about file loading and processing:

```bash
python misc/compute_delta_years.py /path/to/data/ --verbose
```

This will show:
- Individual file loading progress
- Column validation details
- Sample of computation results

### Dependencies

The script requires the endgame-postprocessing package and these dependencies:
- pandas
- numpy
- tqdm

## Using the Docker Container

For easier deployment and reproducibility, you can run this script inside a Docker container.

### Building the Docker Image

Install [Docker](https://docs.docker.com/get-started/get-docker/).

Build the image (this will take a few minutes):

```bash
cd misc/compute_delta_years/
docker build . -t compute-delta-years
```

### Running with Docker

Run the script using Docker with a bind mount to access your data:

```bash
docker run --mount type=bind,src={absolute_path_to_data},dst=/ntdmc/data compute-delta-years /ntdmc/data/canonical_results/ [options]
```

#### Docker Examples

Basic usage:
```bash
docker run --mount type=bind,src=/home/user/oncho_data,dst=/ntdmc/data compute-delta-years \
    /ntdmc/data/canonical_results/ \
    --output /ntdmc/data/delta_years_output.csv
```

Advanced usage with custom parameters:
```bash
docker run --mount type=bind,src=/home/user/disease_data,dst=/ntdmc/data compute-delta-years \
    /ntdmc/data/canonical_results/ \
    --threshold 0.005 \
    --scenarios scenario_1 scenario_2 \
    --reference-scenario scenario_1 \
    --output /ntdmc/data/custom_delta_years.csv \
    --verbose
```

**Note**: 
- Replace `{absolute_path_to_data}` with the full path to your data directory
- The path cannot contain `~`. Use `pwd` to find the full path
- All file paths in the Docker command should be relative to `/ntdmc/data/`

## Integration with Pipeline

This script uses the same core computation function (`compute_delta_years_aggregated`) that is integrated into the main post-processing pipeline. The results should be identical to those produced by the full pipeline, making this useful for:

- **Standalone analysis** of existing canonical results
- **Testing and validation** of delta years computations
- **Custom analysis** with different parameters than the default pipeline
- **Debugging** issues with delta years calculations
- **Containerized deployment** in cloud environments