#!/bin/bash

# Example script for computing delta years for Oncho data
# This demonstrates typical usage patterns for oncho analysis

set -e

# Directory containing canonical results (adjust path as needed)
CANONICAL_DIR="/path/to/oncho/canonical_results"
OUTPUT_FILE="oncho_delta_years.csv"

echo "Computing delta years for Oncho data..."
echo "Input directory: $CANONICAL_DIR"
echo "Output file: $OUTPUT_FILE"

# Basic computation with standard 1% threshold for oncho
python misc/compute_delta_years/compute_delta_years.py \
    "$CANONICAL_DIR" \
    --threshold 0.01 \
    --prevalence-measure processed_prevalence \
    --output "$OUTPUT_FILE" \
    --verbose

echo "Delta years computation completed!"
echo "Results saved to: $OUTPUT_FILE"