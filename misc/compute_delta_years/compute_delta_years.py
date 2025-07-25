#!/usr/bin/env python3
"""
Delta Years Computation Script

This script provides a standalone utility to compute delta years between scenarios
for disease elimination modeling. It reads canonical IU data files and produces
a delta years CSV file showing the year differences when prevalence falls below
a specified threshold for each scenario compared to a reference scenario.

Delta years computation produces two types of measures, with each simulation draw
individually assigned based on whether both scenarios reach the threshold for that specific draw:

For standard comparisons (both scenarios reach threshold for that draw):
- Positive values: Scenario takes longer to reach elimination than the reference
- Negative values: Scenario reaches elimination faster than the reference
- Zero values: Scenario reaches elimination in the same year as the reference

For scenarios that don't reach threshold (atleast_delta_years measures for that draw):
- Positive values: Scenario takes at least this many years longer than the reference
- Negative values: Scenario reaches elimination at least this many years faster than the reference
- 999 values: Neither scenario reaches the threshold, making comparison impossible

Usage:
    python compute_delta_years.py <canonical_dir> [options]

Examples:
    # Basic usage with default settings
    python compute_delta_years.py /path/to/canonical_results/

    # Specify custom threshold and output file
    python compute_delta_years.py /path/to/canonical_results/ --threshold 0.005 --output delta_years_custom.csv

    # Use a specific reference scenario
    python compute_delta_years.py /path/to/canonical_results/ --reference-scenario scenario_2

    # Include only specific scenarios
    python compute_delta_years.py /path/to/canonical_results/ --scenarios scenario_1 scenario_3a

Output Format:
    The output CSV contains columns for:
    - iu_name: Implementation Unit identifier
    - country_code: Country code for the IU
    - scenario: Scenario name
    - measure: Either "delta_years_{reference_scenario}" or "atleast_delta_years_{reference_scenario}"
    - draw_0, draw_1, ..., draw_N: Delta years for each simulation draw

Author: Generated for NTD Modelling Consortium
"""

import argparse
import sys
from pathlib import Path
from typing import List, Optional

import pandas as pd
from tqdm import tqdm


from endgame_postprocessing.post_processing.aggregation import compute_delta_years_aggregated
from endgame_postprocessing.post_processing import canonical_columns
from endgame_postprocessing.post_processing.file_util import post_process_file_generator

def load_canonical_ius(canonical_dir: str, scenarios: Optional[List[str]] = None, verbose: bool = False) -> List[pd.DataFrame]:
    """
    Load canonical IU dataframes from the specified directory.
    
    Args:
        canonical_dir: Path to the canonical results directory
        scenarios: Optional list of specific scenarios to include
        verbose: Whether to show detailed progress information
        
    Returns:
        List of loaded DataFrames
    """
    canonical_path = Path(canonical_dir)
    if not canonical_path.exists():
        raise FileNotFoundError(f"Canonical directory does not exist: {canonical_dir}")
    
    # Collect all file info objects first to show progress
    file_infos = list(post_process_file_generator(
        file_directory=canonical_dir,
        end_of_file="_canonical.csv"
    ))
    
    if not file_infos:
        raise ValueError(f"No canonical files found in {canonical_dir}")
    
    canonical_ius = []
    files_skipped = 0
    
    print(f"Loading canonical IU files from: {canonical_dir}")
    
    # Use tqdm for progress tracking
    progress_bar = tqdm(file_infos, desc="Loading files", unit="file")
    for file_info in progress_bar:
        # Filter by scenarios if specified
        if scenarios and file_info.scenario not in scenarios:
            files_skipped += 1
            if verbose:
                progress_bar.write(f"Skipping {file_info.scenario} (not in specified scenarios)")
            continue
            
        try:
            df = pd.read_csv(file_info.file_path)
            
            # Validate required columns
            required_cols = [
                canonical_columns.SCENARIO,
                canonical_columns.IU_NAME,
                canonical_columns.COUNTRY_CODE,
                canonical_columns.MEASURE,
                canonical_columns.YEAR_ID
            ]
            
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                progress_bar.write(f"Warning: File {file_info.file_path} missing required columns: {missing_cols}")
                files_skipped += 1
                continue
                
            # Check for draw columns
            draw_cols = [col for col in df.columns if col.startswith('draw_')]
            if not draw_cols:
                progress_bar.write(f"Warning: File {file_info.file_path} has no draw columns")
                files_skipped += 1
                continue
                
            canonical_ius.append(df)
            
            if verbose:
                progress_bar.write(f"Loaded: {file_info.scenario}/{file_info.iu} ({len(draw_cols)} draws)")
            
        except Exception as e:
            progress_bar.write(f"Error loading {file_info.file_path}: {e}")
            files_skipped += 1
            continue
    
    progress_bar.close()
    
    if not canonical_ius:
        raise ValueError("No valid canonical IU files could be loaded")
    
    print(f"Successfully loaded {len(canonical_ius)} canonical IU files")
    print(f"  Files processed: {len(file_infos)}")
    print(f"  Files loaded: {len(canonical_ius)}")
    print(f"  Files skipped: {files_skipped}")
    
    return canonical_ius


def get_scenarios_summary(canonical_ius: List[pd.DataFrame]) -> None:
    """Print a summary of scenarios and IUs found in the data."""
    scenarios = set()
    ius_by_scenario = {}
    
    for df in canonical_ius:
        scenario = df[canonical_columns.SCENARIO].iloc[0]
        scenarios.add(scenario)
        
        iu_name = df[canonical_columns.IU_NAME].iloc[0]
        if scenario not in ius_by_scenario:
            ius_by_scenario[scenario] = set()
        ius_by_scenario[scenario].add(iu_name)
    
    print(f"\nData Summary:")
    print(f"  Scenarios found: {sorted(scenarios)}")
    print(f"  Total canonical IU files: {len(canonical_ius)}")
    
    for scenario in sorted(scenarios):
        ius_count = len(ius_by_scenario[scenario])
        print(f"    {scenario}: {ius_count} IUs")


def main():
    """Main function to run the delta years computation."""
    parser = argparse.ArgumentParser(
        description="Compute delta years between scenarios from canonical IU data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s /path/to/canonical_results/
  %(prog)s /path/to/canonical_results/ --threshold 0.005 --output custom_delta.csv
  %(prog)s /path/to/canonical_results/ --reference-scenario scenario_2
  %(prog)s /path/to/canonical_results/ --scenarios scenario_1 scenario_3a

Output:
  Creates a CSV file with delta years data showing the year difference between
  when each scenario reaches the elimination threshold compared to a reference
  scenario, computed for every simulation draw at the IU level.
        """
    )
    
    parser.add_argument(
        'canonical_dir',
        help='Path to the directory containing canonical results'
    )
    
    parser.add_argument(
        '--threshold',
        type=float,
        default=0.01,
        help='Prevalence threshold for elimination (default: 0.01)'
    )
    
    parser.add_argument(
        '--reference-scenario',
        help='Reference scenario name (default: auto-detect first scenario)'
    )
    
    parser.add_argument(
        '--prevalence-measure',
        default='processed_prevalence',
        help='Name of the prevalence measure column (default: processed_prevalence)'
    )
    
    parser.add_argument(
        '--output',
        default='delta_years_output.csv',
        help='Output CSV file name (default: delta_years_output.csv)'
    )
    
    parser.add_argument(
        '--scenarios',
        nargs='+',
        help='Specific scenarios to include (default: all scenarios found)'
    )
    
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Print detailed progress information'
    )
    
    args = parser.parse_args()
    
    try:
        # Load canonical IUs
        canonical_ius = load_canonical_ius(args.canonical_dir, args.scenarios, args.verbose)
        
        # Show data summary
        get_scenarios_summary(canonical_ius)
        
        # Compute delta years
        print(f"\nComputing delta years...")
        print(f"  Threshold: {args.threshold}")
        print(f"  Prevalence measure: {args.prevalence_measure}")
        print(f"  Reference scenario: {args.reference_scenario or 'auto-detect'}")
        
        # Show progress for delta years computation
        with tqdm(desc="Computing delta years", unit="computation") as pbar:
            delta_years_df = compute_delta_years_aggregated(
                canonical_ius=canonical_ius,
                threshold=args.threshold,
                reference_scenario=args.reference_scenario,
                prevalence_measure=args.prevalence_measure
            )
            pbar.update(1)
        
        # Save results
        output_path = Path(args.output)
        print(f"\nSaving results to: {output_path.absolute()}")
        
        with tqdm(desc="Writing CSV", unit="row") as pbar:
            delta_years_df.to_csv(output_path, index=False, float_format='%g')
            pbar.update(len(delta_years_df))
        
        print(f"\n✅ Delta years computation completed successfully!")
        print(f"   Output saved to: {output_path.absolute()}")
        print(f"   Rows: {len(delta_years_df):,}")
        print(f"   Columns: {len(delta_years_df.columns):,}")
        
        # Show sample of results
        if not delta_years_df.empty:
            scenarios = delta_years_df['scenario'].unique()
            reference_scenario_used = delta_years_df['measure'].iloc[0].replace('delta_years_', '')
            print(f"   Reference scenario used: {reference_scenario_used}")
            print(f"   Scenarios in results: {sorted(scenarios)}")
            
            if args.verbose:
                print(f"\nSample of results:")
                sample_cols = ['iu_name', 'country_code', 'scenario', 'measure'] + [col for col in delta_years_df.columns if col.startswith('draw_')][:5]
                print(delta_years_df[sample_cols].head().to_string(index=False))
        
    except Exception as e:
        print(f"❌ Error: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
