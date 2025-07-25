#!/bin/bash

# Example script showing advanced usage with custom parameters
# This demonstrates filtering scenarios and using custom reference scenarios

set -e

# Directory containing canonical results (adjust path as needed)
CANONICAL_DIR="/path/to/canonical_results"
OUTPUT_FILE="custom_delta_years.csv"

echo "Computing delta years with custom parameters..."
echo "Input directory: $CANONICAL_DIR"
echo "Output file: $OUTPUT_FILE"

# Advanced usage example:
# - Custom threshold (0.5% instead of 1%)
# - Specific scenarios only
# - Custom reference scenario
# - Custom output file name
# Output will contain two measure types per IU-scenario:
# - delta_years_*: draws where both scenarios reach threshold
# - atleast_delta_years_*: draws where at least one scenario doesn't reach threshold
python misc/compute_delta_years/compute_delta_years.py \
    "$CANONICAL_DIR" \
    --threshold 0.005 \
    --scenarios scenario_1 scenario_2 scenario_3a \
    --reference-scenario scenario_2 \
    --prevalence-measure processed_prevalence \
    --output "$OUTPUT_FILE" \
    --verbose

echo "Custom delta years computation completed!"
echo "Results saved to: $OUTPUT_FILE"

# Show a quick summary of the results
echo ""
echo "Quick summary of results:"
echo "Number of rows: $(tail -n +2 "$OUTPUT_FILE" | wc -l)"
echo "Number of columns: $(head -1 "$OUTPUT_FILE" | tr ',' '\n' | wc -l)"
echo "Scenarios in results: $(tail -n +2 "$OUTPUT_FILE" | cut -d',' -f3 | sort -u | tr '\n' ' ')"