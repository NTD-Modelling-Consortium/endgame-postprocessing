#!/bin/bash

# Example script for computing delta years for Trachoma data
# Trachoma uses a different threshold (5% = 0.05) and different scenario naming

set -e

# Directory containing canonical results (adjust path as needed)
CANONICAL_DIR="/path/to/trachoma/canonical_results"
OUTPUT_FILE="trachoma_delta_years.csv"

echo "Computing delta years for Trachoma data..."
echo "Input directory: $CANONICAL_DIR"
echo "Output file: $OUTPUT_FILE"

# Trachoma analysis with 5% threshold (standard for trachoma)
# Trachoma often uses scenarios like scenario_1_5, scenario_2, etc.
python misc/compute_delta_years/compute_delta_years.py \
    "$CANONICAL_DIR" \
    --threshold 0.05 \
    --prevalence-measure processed_prevalence \
    --output "$OUTPUT_FILE" \
    --verbose

echo "Delta years computation completed!"
echo "Results saved to: $OUTPUT_FILE"