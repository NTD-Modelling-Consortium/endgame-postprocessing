#!/bin/bash

# Example script for computing delta years for LF data
# LF typically uses different scenario names (scenario_0, scenario_minus1, etc.)

set -e

# Directory containing canonical results (adjust path as needed)
CANONICAL_DIR="/path/to/lf/canonical_results"
OUTPUT_FILE="lf_delta_years.csv"

echo "Computing delta years for LF data..."
echo "Input directory: $CANONICAL_DIR"
echo "Output file: $OUTPUT_FILE"

# LF analysis with standard 1% threshold
# Auto-detects reference scenario from available scenarios
python misc/compute_delta_years/compute_delta_years.py \
    "$CANONICAL_DIR" \
    --threshold 0.01 \
    --prevalence-measure processed_prevalence \
    --output "$OUTPUT_FILE" \
    --verbose

echo "Delta years computation completed!"
echo "Results saved to: $OUTPUT_FILE"