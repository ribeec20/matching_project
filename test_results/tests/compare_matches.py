"""Compare NDA-ANDA matches from our pipeline with collected_data_final.xlsx.

This script compares:
1. Which NDAs have matches in both datasets
2. The earliest ANDA match for each NDA
3. The calculated monopoly time differences
4. Matches found by pipeline but not in collected data (and vice versa)
"""

import pandas as pd
import numpy as np
from datetime import datetime

# File paths
COLLECTED_DATA_PATH = "collected_data_final.xlsx"
PIPELINE_MATCHES_PATH = "final_nda_anda_matches.txt"
MONOPOLY_TIMES_PATH = "monopoly_times_from_matches.csv"
ORANGE_BOOK_PATH = "OB - Products - Dec 2018.xlsx"
OUTPUT_COMPARISON_PATH = "match_comparison_report.csv"
OUTPUT_TOP5_COMPARISON_PATH = "top5_anda_comparison.txt"


def load_collected_data(filepath: str) -> pd.DataFrame:
    """Load and preprocess the collected_data_final.xlsx file.
    
    Args:
        filepath: Path to collected_data_final.xlsx
        
    Returns:
        DataFrame with collected data
    """
    print(f"Loading collected data from {filepath}...")
    df = pd.read_excel(filepath)
    
    # Convert ANDA Approval Date to datetime
    df['ANDA Approval Date'] = pd.to_datetime(df['ANDA Approval Date'], errors='coerce')
    
    # Convert NDA Approval Date to datetime too
    df['NDA Approval Date'] = pd.to_datetime(df['NDA Approval Date'], errors='coerce')
    
    # Filter out rows without ANDA matches (NaN or 'OTC')
    df = df[df['ANDA'].notna()]
    df = df[df['ANDA'] != 'OTC']
    
    # Convert ANDA to numeric, removing any non-numeric values
    df['ANDA'] = pd.to_numeric(df['ANDA'], errors='coerce')
    df = df[df['ANDA'].notna()]
    df['ANDA'] = df['ANDA'].astype(int)
    
    print(f"  Loaded {len(df)} NDA-ANDA match records")
    print(f"  Unique NDAs: {df['NDA'].nunique()}")
    print(f"  Unique ANDAs: {df['ANDA'].nunique()}")
    
    return df


def parse_pipeline_matches(filepath: str) -> dict:
    """Parse final_nda_anda_matches.txt to extract NDA-ANDA mappings.
    
    Args:
        filepath: Path to final_nda_anda_matches.txt
        
    Returns:
        Dictionary mapping NDA number to list of ANDA numbers
    """
    print(f"\nLoading pipeline matches from {filepath}...")
    nda_anda_map = {}
    
    with open(filepath, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            
            # Skip header lines and empty lines
            if not line or line.startswith('=') or line.startswith('-') or ':' not in line:
                continue
            
            # Skip metadata lines
            if line.startswith('Generated:') or line.startswith('Total'):
                continue
            
            # Parse NDA: ANDA1, ANDA2, ... format
            if line.startswith('NDA'):
                parts = line.split(':', 1)
                if len(parts) == 2:
                    nda_num = int(parts[0].replace('NDA', '').strip())
                    anda_list_str = parts[1].strip()
                    
                    # Split by comma and get unique ANDAs
                    andas = [int(anda.strip()) for anda in anda_list_str.split(',')]
                    unique_andas = sorted(set(andas))
                    
                    nda_anda_map[nda_num] = unique_andas
    
    total_matches = sum(len(andas) for andas in nda_anda_map.values())
    print(f"  Loaded {len(nda_anda_map)} NDAs with matches")
    print(f"  Total unique ANDA matches: {total_matches}")
    
    return nda_anda_map


def load_monopoly_times(filepath: str) -> pd.DataFrame:
    """Load monopoly times calculated from pipeline.
    
    Args:
        filepath: Path to monopoly_times_from_matches.csv
        
    Returns:
        DataFrame with monopoly times
    """
    print(f"\nLoading pipeline monopoly times from {filepath}...")
    df = pd.read_csv(filepath)
    
    # Convert to numeric
    df['NDA_Appl_No'] = df['NDA_Appl_No'].astype(int)
    df['Earliest_ANDA_Number'] = df['Earliest_ANDA_Number'].astype(int)
    df['NDA_Approval_Date'] = pd.to_datetime(df['NDA_Approval_Date'])
    df['Earliest_ANDA_Date'] = pd.to_datetime(df['Earliest_ANDA_Date'])
    
    print(f"  Loaded monopoly times for {len(df)} NDAs")
    
    return df


def get_earliest_anda_per_nda(collected_data: pd.DataFrame) -> pd.DataFrame:
    """Get the ANDA with the SHORTEST monopoly time for each NDA from collected data.
    
    IMPORTANT: This finds the ANDA that gives the MINIMUM monopoly period,
    not just the earliest approval date. This matches what our pipeline does.
    
    Args:
        collected_data: DataFrame with collected data
        
    Returns:
        DataFrame with shortest monopoly time ANDA per NDA
    """
    print("\nFinding ANDA with SHORTEST monopoly time per NDA from collected data...")
    print("  (Using 'Length of Monopoly Period (Days)' to find minimum monopoly time)")
    
    # Filter to only rows with valid monopoly period data
    valid_data = collected_data[
        collected_data['Length of Monopoly Period (Days)'].notna()
    ].copy()
    
    print(f"  Filtered to {len(valid_data)} records with valid monopoly period data")
    
    # METHOD 1: Find ANDA with MINIMUM monopoly period per NDA (CORRECT)
    # This uses the pre-calculated monopoly periods in the collected data
    shortest_monopoly = valid_data.loc[
        valid_data.groupby('NDA')['Length of Monopoly Period (Days)'].idxmin()
    ]
    
    # Also get earliest by date for comparison
    valid_by_date = collected_data[collected_data['ANDA Approval Date'].notna()].copy()
    earliest_by_date = valid_by_date.loc[
        valid_by_date.groupby('NDA')['ANDA Approval Date'].idxmin()
    ]
    
    # Compare the two methods
    ndas_in_both = set(shortest_monopoly['NDA']) & set(earliest_by_date['NDA'])
    disagreements = 0
    
    for nda in ndas_in_both:
        shortest_anda = shortest_monopoly[shortest_monopoly['NDA'] == nda]['ANDA'].values[0]
        earliest_anda = earliest_by_date[earliest_by_date['NDA'] == nda]['ANDA'].values[0]
        
        if shortest_anda != earliest_anda:
            disagreements += 1
    
    if disagreements > 0:
        print(f"  ⚠️  WARNING: {disagreements} NDAs have different ANDAs when using:")
        print(f"      - Minimum monopoly period vs Earliest approval date")
        print(f"      Using MINIMUM MONOPOLY PERIOD (correct approach)")
    
    shortest_monopoly = shortest_monopoly[['NDA', 'ANDA', 'ANDA Approval Date', 
                        'Length of Monopoly Period (Days)', 
                        'Length of Monopoly Period (Years)']].copy()
    
    shortest_monopoly.columns = ['NDA', 'Earliest_ANDA_Collected', 'Earliest_ANDA_Date_Collected',
                       'Monopoly_Days_Collected', 'Monopoly_Years_Collected']
    
    print(f"  Found shortest monopoly time ANDA for {len(shortest_monopoly)} NDAs")
    
    return shortest_monopoly


def compare_matches(pipeline_map: dict, collected_data: pd.DataFrame, 
                   pipeline_monopoly: pd.DataFrame) -> pd.DataFrame:
    """Compare matches between pipeline and collected data.
    
    Args:
        pipeline_map: Dictionary of NDA -> list of ANDAs from pipeline
        collected_data: DataFrame with collected data
        pipeline_monopoly: DataFrame with pipeline monopoly times
        
    Returns:
        DataFrame with comparison results
    """
    print("\nComparing matches between pipeline and collected data...")
    
    # Get earliest ANDA from collected data
    collected_earliest = get_earliest_anda_per_nda(collected_data)
    
    # Get all unique NDAs from both sources
    pipeline_ndas = set(pipeline_map.keys())
    collected_ndas = set(collected_data['NDA'].unique())
    all_ndas = pipeline_ndas | collected_ndas
    
    print(f"\n  NDAs in pipeline only: {len(pipeline_ndas - collected_ndas)}")
    print(f"  NDAs in collected data only: {len(collected_ndas - pipeline_ndas)}")
    print(f"  NDAs in both: {len(pipeline_ndas & collected_ndas)}")
    
    # Build comparison DataFrame
    comparison_rows = []
    
    for nda in sorted(all_ndas):
        row = {'NDA': nda}
        
        # Pipeline data
        if nda in pipeline_map:
            row['In_Pipeline'] = True
            row['Pipeline_ANDA_Count'] = len(pipeline_map[nda])
            row['Pipeline_ANDAs'] = ' | '.join(map(str, pipeline_map[nda]))
            
            # Get monopoly time from pipeline
            pipeline_match = pipeline_monopoly[pipeline_monopoly['NDA_Appl_No'] == nda]
            if not pipeline_match.empty:
                row['Pipeline_Earliest_ANDA'] = int(pipeline_match.iloc[0]['Earliest_ANDA_Number'])
                row['Pipeline_Earliest_ANDA_Date'] = pipeline_match.iloc[0]['Earliest_ANDA_Date']
                row['Pipeline_Monopoly_Days'] = pipeline_match.iloc[0]['Actual_Monopoly_Days']
                row['Pipeline_Monopoly_Years'] = pipeline_match.iloc[0]['Actual_Monopoly_Years']
            else:
                row['Pipeline_Earliest_ANDA'] = np.nan
                row['Pipeline_Earliest_ANDA_Date'] = pd.NaT
                row['Pipeline_Monopoly_Days'] = np.nan
                row['Pipeline_Monopoly_Years'] = np.nan
        else:
            row['In_Pipeline'] = False
            row['Pipeline_ANDA_Count'] = 0
            row['Pipeline_ANDAs'] = ''
            row['Pipeline_Earliest_ANDA'] = np.nan
            row['Pipeline_Earliest_ANDA_Date'] = pd.NaT
            row['Pipeline_Monopoly_Days'] = np.nan
            row['Pipeline_Monopoly_Years'] = np.nan
        
        # Collected data
        collected_matches = collected_data[collected_data['NDA'] == nda]
        if not collected_matches.empty:
            row['In_Collected'] = True
            row['Collected_ANDA_Count'] = len(collected_matches)
            row['Collected_ANDAs'] = ' | '.join(map(str, sorted(collected_matches['ANDA'].unique())))
            
            # Get earliest ANDA from collected data
            earliest = collected_earliest[collected_earliest['NDA'] == nda]
            if not earliest.empty:
                row['Collected_Earliest_ANDA'] = int(earliest.iloc[0]['Earliest_ANDA_Collected'])
                row['Collected_Earliest_ANDA_Date'] = earliest.iloc[0]['Earliest_ANDA_Date_Collected']
                row['Collected_Monopoly_Days'] = earliest.iloc[0]['Monopoly_Days_Collected']
                row['Collected_Monopoly_Years'] = earliest.iloc[0]['Monopoly_Years_Collected']
            else:
                row['Collected_Earliest_ANDA'] = np.nan
                row['Collected_Earliest_ANDA_Date'] = pd.NaT
                row['Collected_Monopoly_Days'] = np.nan
                row['Collected_Monopoly_Years'] = np.nan
        else:
            row['In_Collected'] = False
            row['Collected_ANDA_Count'] = 0
            row['Collected_ANDAs'] = ''
            row['Collected_Earliest_ANDA'] = np.nan
            row['Collected_Earliest_ANDA_Date'] = pd.NaT
            row['Collected_Monopoly_Days'] = np.nan
            row['Collected_Monopoly_Years'] = np.nan
        
        comparison_rows.append(row)
    
    comparison_df = pd.DataFrame(comparison_rows)
    
    # Calculate differences
    comparison_df['Earliest_ANDA_Match'] = (
        comparison_df['Pipeline_Earliest_ANDA'] == comparison_df['Collected_Earliest_ANDA']
    )
    
    comparison_df['Monopoly_Days_Difference'] = (
        comparison_df['Pipeline_Monopoly_Days'] - comparison_df['Collected_Monopoly_Days']
    )
    
    comparison_df['Monopoly_Years_Difference'] = (
        comparison_df['Pipeline_Monopoly_Years'] - comparison_df['Collected_Monopoly_Years']
    )
    
    # Categorize comparison results
    def categorize(row):
        if row['In_Pipeline'] and row['In_Collected']:
            if row['Earliest_ANDA_Match']:
                return 'Both_Same_Earliest'
            else:
                return 'Both_Different_Earliest'
        elif row['In_Pipeline']:
            return 'Pipeline_Only'
        elif row['In_Collected']:
            return 'Collected_Only'
        else:
            return 'Unknown'
    
    comparison_df['Match_Category'] = comparison_df.apply(categorize, axis=1)
    
    return comparison_df


def print_comparison_summary(comparison_df: pd.DataFrame):
    """Print a summary of the comparison results.
    
    Args:
        comparison_df: DataFrame with comparison results
    """
    print("\n" + "="*80)
    print("COMPARISON SUMMARY")
    print("="*80)
    
    print(f"\nTotal NDAs analyzed: {len(comparison_df)}")
    
    # Category breakdown
    print("\nMatch Categories:")
    for category, count in comparison_df['Match_Category'].value_counts().items():
        pct = 100 * count / len(comparison_df)
        print(f"  {category}: {count} ({pct:.1f}%)")
    
    # NDAs in both datasets
    both_datasets = comparison_df[comparison_df['In_Pipeline'] & comparison_df['In_Collected']]
    print(f"\n\nNDAs in BOTH datasets: {len(both_datasets)}")
    
    if len(both_datasets) > 0:
        # Earliest ANDA agreement - ENHANCED
        same_earliest = both_datasets[both_datasets['Earliest_ANDA_Match'] == True]
        different_earliest = both_datasets[both_datasets['Earliest_ANDA_Match'] == False]
        
        print(f"\n  EARLIEST ANDA COMPARISON:")
        print(f"    [SAME] Same earliest ANDA: {len(same_earliest)} ({100*len(same_earliest)/len(both_datasets):.1f}%)")
        print(f"    [DIFF] Different earliest ANDA: {len(different_earliest)} ({100*len(different_earliest)/len(both_datasets):.1f}%)")
        
        # Analyze why they differ
        if len(different_earliest) > 0:
            # Check if pipeline found earlier ANDAs
            pipeline_earlier = different_earliest[
                different_earliest['Pipeline_Earliest_ANDA_Date'] < different_earliest['Collected_Earliest_ANDA_Date']
            ]
            collected_earlier = different_earliest[
                different_earliest['Pipeline_Earliest_ANDA_Date'] > different_earliest['Collected_Earliest_ANDA_Date']
            ]
            
            print(f"\n    Breakdown of differences:")
            print(f"      Pipeline found earlier ANDA: {len(pipeline_earlier)} cases")
            print(f"      Collected data has earlier ANDA: {len(collected_earlier)} cases")
        
        # Monopoly time comparison
        valid_monopoly = both_datasets[
            both_datasets['Pipeline_Monopoly_Years'].notna() & 
            both_datasets['Collected_Monopoly_Years'].notna()
        ]
        
        if len(valid_monopoly) > 0:
            print(f"\n  MONOPOLY TIME COMPARISON ({len(valid_monopoly)} NDAs with both values):")
            print(f"    Mean difference: {valid_monopoly['Monopoly_Years_Difference'].mean():.2f} years")
            print(f"    Median difference: {valid_monopoly['Monopoly_Years_Difference'].median():.2f} years")
            print(f"    Std deviation: {valid_monopoly['Monopoly_Years_Difference'].std():.2f} years")
            print(f"    Min difference: {valid_monopoly['Monopoly_Years_Difference'].min():.2f} years")
            print(f"    Max difference: {valid_monopoly['Monopoly_Years_Difference'].max():.2f} years")
            
            # Show agreement level
            close_agreement = valid_monopoly[abs(valid_monopoly['Monopoly_Years_Difference']) < 0.1]
            print(f"\n    Agreement within 0.1 years: {len(close_agreement)} ({100*len(close_agreement)/len(valid_monopoly):.1f}%)")
    
    # Examples of differences
    print("\n" + "="*80)
    print("DETAILED EARLIEST ANDA COMPARISON")
    print("="*80)
    
    # Show cases where earliest ANDA matches
    same_earliest_detailed = comparison_df[
        (comparison_df['Match_Category'] == 'Both_Same_Earliest') &
        comparison_df['Pipeline_Earliest_ANDA'].notna() &
        comparison_df['Collected_Earliest_ANDA'].notna()
    ].head(10)
    
    if not same_earliest_detailed.empty:
        print("\n[SAME] SAME EARLIEST ANDA (first 10):")
        print("NDA    | Earliest ANDA | Pipeline Date  | Collected Date | Pipeline Monopoly | Collected Monopoly")
        print("-" * 110)
        for _, row in same_earliest_detailed.iterrows():
            pipeline_date = pd.to_datetime(row['Pipeline_Earliest_ANDA_Date']).strftime('%Y-%m-%d') if pd.notna(row['Pipeline_Earliest_ANDA_Date']) else 'N/A'
            collected_date = pd.to_datetime(row['Collected_Earliest_ANDA_Date']).strftime('%Y-%m-%d') if pd.notna(row['Collected_Earliest_ANDA_Date']) else 'N/A'
            print(f"{int(row['NDA']):6d} | "
                  f"ANDA {int(row['Pipeline_Earliest_ANDA']):6d} | "
                  f"{pipeline_date:10s} | "
                  f"{collected_date:10s} | "
                  f"{row['Pipeline_Monopoly_Years']:6.2f} years      | "
                  f"{row['Collected_Monopoly_Years']:6.2f} years")
    
    # Show cases where earliest ANDA differs
    different_earliest = comparison_df[
        (comparison_df['Match_Category'] == 'Both_Different_Earliest') &
        comparison_df['Pipeline_Earliest_ANDA'].notna() &
        comparison_df['Collected_Earliest_ANDA'].notna()
    ]
    
    if not different_earliest.empty:
        print(f"\n[DIFF] DIFFERENT EARLIEST ANDA (showing all {len(different_earliest)} cases):")
        print("NDA    | Pipeline ANDA (Date)       | Collected ANDA (Date)      | Which Earlier? | Pipeline Mono | Collected Mono | Diff")
        print("-" * 135)
        for _, row in different_earliest.iterrows():
            pipeline_date = pd.to_datetime(row['Pipeline_Earliest_ANDA_Date']).strftime('%Y-%m-%d') if pd.notna(row['Pipeline_Earliest_ANDA_Date']) else 'N/A'
            collected_date = pd.to_datetime(row['Collected_Earliest_ANDA_Date']).strftime('%Y-%m-%d') if pd.notna(row['Collected_Earliest_ANDA_Date']) else 'N/A'
            
            # Determine which is earlier
            if pd.notna(row['Pipeline_Earliest_ANDA_Date']) and pd.notna(row['Collected_Earliest_ANDA_Date']):
                if pd.to_datetime(row['Pipeline_Earliest_ANDA_Date']) < pd.to_datetime(row['Collected_Earliest_ANDA_Date']):
                    which_earlier = "Pipeline ✓"
                elif pd.to_datetime(row['Pipeline_Earliest_ANDA_Date']) > pd.to_datetime(row['Collected_Earliest_ANDA_Date']):
                    which_earlier = "Collected ✓"
                else:
                    which_earlier = "Same Date"
            else:
                which_earlier = "N/A"
            
            print(f"{int(row['NDA']):6d} | "
                  f"ANDA {int(row['Pipeline_Earliest_ANDA']):6d} ({pipeline_date}) | "
                  f"ANDA {int(row['Collected_Earliest_ANDA']):6d} ({collected_date}) | "
                  f"{which_earlier:14s} | "
                  f"{row['Pipeline_Monopoly_Years']:6.2f} years  | "
                  f"{row['Collected_Monopoly_Years']:6.2f} years   | "
                  f"{row['Monopoly_Years_Difference']:+5.2f}y")
    
    # Original examples section
    print("\n" + "="*80)
    print("DATASET-SPECIFIC MATCHES")
    print("="*80)
    
    # Pipeline only
    pipeline_only = comparison_df[comparison_df['Match_Category'] == 'Pipeline_Only'].head(10)
    if not pipeline_only.empty:
        print(f"\n\nNDAs found by PIPELINE ONLY (first 10 of {len(comparison_df[comparison_df['Match_Category'] == 'Pipeline_Only'])}):")
        print("NDA    | Earliest ANDA | Monopoly Time | ANDA Count")
        print("-" * 80)
        for _, row in pipeline_only.iterrows():
            anda_str = f"ANDA {int(row['Pipeline_Earliest_ANDA']):6d}" if pd.notna(row['Pipeline_Earliest_ANDA']) else "N/A"
            mono_str = f"{row['Pipeline_Monopoly_Years']:6.2f} years" if pd.notna(row['Pipeline_Monopoly_Years']) else "N/A"
            print(f"{int(row['NDA']):6d} | {anda_str:13s} | {mono_str:13s} | {int(row['Pipeline_ANDA_Count']):10d}")
    
    # Collected only
    collected_only = comparison_df[comparison_df['Match_Category'] == 'Collected_Only'].head(10)
    if not collected_only.empty:
        print(f"\n\nNDAs found by COLLECTED DATA ONLY (first 10 of {len(comparison_df[comparison_df['Match_Category'] == 'Collected_Only'])}):")
        print("NDA    | Earliest ANDA | Monopoly Time | ANDA Count")
        print("-" * 80)
        for _, row in collected_only.iterrows():
            anda_str = f"ANDA {int(row['Collected_Earliest_ANDA']):6d}" if pd.notna(row['Collected_Earliest_ANDA']) else "N/A"
            mono_str = f"{row['Collected_Monopoly_Years']:6.2f} years" if pd.notna(row['Collected_Monopoly_Years']) else "N/A"
            print(f"{int(row['NDA']):6d} | {anda_str:13s} | {mono_str:13s} | {int(row['Collected_ANDA_Count']):10d}")


def analyze_top5_matches(collected_data: pd.DataFrame, collected_earliest: pd.DataFrame, 
                        pipeline_map: dict, nda_approval_dates: dict) -> str:
    """Analyze if the correct ANDA appears in the top 5 earliest ANDAs from pipeline.
    
    Args:
        collected_data: Full collected data DataFrame
        collected_earliest: DataFrame with earliest ANDA per NDA from collected data
        pipeline_map: Dictionary of NDA -> list of ANDAs from pipeline
        nda_approval_dates: Dictionary of NDA -> approval date
        
    Returns:
        String with detailed analysis
    """
    output_lines = []
    output_lines.append("=" * 100)
    output_lines.append("TOP 5 ANDA COMPARISON: CORRECT ANDA vs PIPELINE TOP 5 EARLIEST")
    output_lines.append("=" * 100)
    output_lines.append("")
    output_lines.append("Legend:")
    output_lines.append("  [EXACT MATCH] - The correct ANDA is rank #1 in pipeline")
    output_lines.append("  [IN TOP 5] - The correct ANDA appears in pipeline's top 5 (rank 2-5)")
    output_lines.append("  [NOT IN TOP 5] - The correct ANDA is not in pipeline's top 5")
    output_lines.append("  [NOT FOUND] - The correct ANDA was not found by pipeline at all")
    output_lines.append("")
    output_lines.append("=" * 100)
    output_lines.append("")
    
    # Get NDAs that appear in both datasets
    both_ndas = set(collected_earliest['NDA']) & set(pipeline_map.keys())
    
    exact_matches = 0
    in_top5 = 0
    not_in_top5 = 0
    not_found = 0
    
    for nda in sorted(both_ndas):
        # Get correct ANDA from collected data
        correct_row = collected_earliest[collected_earliest['NDA'] == nda].iloc[0]
        correct_anda = int(correct_row['Earliest_ANDA_Collected'])
        correct_date = correct_row['Earliest_ANDA_Date_Collected']
        correct_monopoly_years = correct_row['Monopoly_Years_Collected']
        
        # Get NDA approval date
        nda_approval = nda_approval_dates.get(nda)
        
        # Get all ANDAs from pipeline for this NDA
        pipeline_andas = pipeline_map[nda]
        
        # Get approval dates for all pipeline ANDAs from collected data
        anda_dates = []
        for anda in pipeline_andas:
            anda_rows = collected_data[(collected_data['NDA'] == nda) & (collected_data['ANDA'] == anda)]
            if not anda_rows.empty:
                anda_date = anda_rows.iloc[0]['ANDA Approval Date']
                anda_monopoly_days = anda_rows.iloc[0]['Length of Monopoly Period (Days)']
                if pd.notna(anda_date) and pd.notna(nda_approval):
                    monopoly_days = (pd.to_datetime(anda_date) - pd.to_datetime(nda_approval)).days
                    anda_dates.append((anda, anda_date, monopoly_days))
        
        # Sort by date to get top 5 earliest
        anda_dates.sort(key=lambda x: x[1])
        top5 = anda_dates[:5]
        
        # Check if correct ANDA is in top 5
        correct_rank = None
        for i, (anda, date, mono) in enumerate(top5, 1):
            if anda == correct_anda:
                correct_rank = i
                break
        
        # Determine status
        if correct_rank == 1:
            status = "[EXACT MATCH]"
            exact_matches += 1
        elif correct_rank is not None:
            status = f"[IN TOP 5 - Rank #{correct_rank}]"
            in_top5 += 1
        elif correct_anda in pipeline_andas:
            status = "[NOT IN TOP 5]"
            not_in_top5 += 1
        else:
            status = "[NOT FOUND]"
            not_found += 1
        
        # Format output
        output_lines.append(f"NDA {nda}")
        output_lines.append(f"  Status: {status}")
        output_lines.append(f"  Correct ANDA: {correct_anda} (approved {correct_date.strftime('%Y-%m-%d')}, monopoly: {correct_monopoly_years:.2f} years)")
        output_lines.append(f"")
        output_lines.append(f"  Pipeline Top 5 Earliest ANDAs:")
        
        for i, (anda, date, mono) in enumerate(top5, 1):
            marker = " <-- CORRECT" if anda == correct_anda else ""
            monopoly_years = mono / 365.25
            output_lines.append(f"    {i}. ANDA {anda:6d} - {date.strftime('%Y-%m-%d')} (monopoly: {monopoly_years:6.2f} years){marker}")
        
        output_lines.append("")
        output_lines.append("-" * 100)
        output_lines.append("")
    
    # Summary statistics
    total = len(both_ndas)
    output_lines.append("")
    output_lines.append("=" * 100)
    output_lines.append("SUMMARY STATISTICS")
    output_lines.append("=" * 100)
    output_lines.append(f"")
    output_lines.append(f"Total NDAs in both datasets: {total}")
    output_lines.append(f"")
    output_lines.append(f"  [EXACT MATCH]    Correct ANDA is rank #1: {exact_matches:3d} ({100*exact_matches/total:5.1f}%)")
    output_lines.append(f"  [IN TOP 5]       Correct ANDA in ranks 2-5: {in_top5:3d} ({100*in_top5/total:5.1f}%)")
    output_lines.append(f"  [NOT IN TOP 5]   Correct ANDA beyond rank 5: {not_in_top5:3d} ({100*not_in_top5/total:5.1f}%)")
    output_lines.append(f"  [NOT FOUND]      Correct ANDA not in pipeline: {not_found:3d} ({100*not_found/total:5.1f}%)")
    output_lines.append(f"")
    output_lines.append(f"  Combined Top 5 accuracy: {exact_matches + in_top5:3d} ({100*(exact_matches + in_top5)/total:5.1f}%)")
    output_lines.append("")
    output_lines.append("=" * 100)
    
    return "\n".join(output_lines)


def main():
    """Main comparison function."""
    print("="*80)
    print("NDA-ANDA MATCH COMPARISON")
    print("Comparing Pipeline Results vs Collected Data")
    print("="*80)
    
    # Load data
    collected_data = load_collected_data(COLLECTED_DATA_PATH)
    pipeline_map = parse_pipeline_matches(PIPELINE_MATCHES_PATH)
    pipeline_monopoly = load_monopoly_times(MONOPOLY_TIMES_PATH)
    
    # Get NDA approval dates from collected data
    nda_approval_dates = {}
    for nda in collected_data['NDA'].unique():
        nda_rows = collected_data[collected_data['NDA'] == nda]
        if not nda_rows.empty:
            nda_approval = nda_rows.iloc[0]['NDA Approval Date']
            if pd.notna(nda_approval):
                nda_approval_dates[nda] = nda_approval
    
    # Get earliest ANDA per NDA from collected data
    collected_earliest = get_earliest_anda_per_nda(collected_data)
    
    # Compare
    comparison_df = compare_matches(pipeline_map, collected_data, pipeline_monopoly)
    
    # Print summary
    print_comparison_summary(comparison_df)
    
    # Save results
    print(f"\n\nSaving detailed comparison to {OUTPUT_COMPARISON_PATH}...")
    comparison_df.to_csv(OUTPUT_COMPARISON_PATH, index=False)
    print(f"[SAVED] Comparison report with {len(comparison_df)} NDAs")
    
    # Generate top 5 analysis
    print(f"\n\nGenerating top 5 ANDA comparison analysis...")
    top5_analysis = analyze_top5_matches(collected_data, collected_earliest, pipeline_map, nda_approval_dates)
    
    # Save top 5 analysis to text file
    print(f"Saving top 5 analysis to {OUTPUT_TOP5_COMPARISON_PATH}...")
    with open(OUTPUT_TOP5_COMPARISON_PATH, 'w', encoding='utf-8') as f:
        f.write(top5_analysis)
    print(f"[SAVED] Top 5 comparison analysis")
    
    print("\n" + "="*80)
    print("COMPARISON COMPLETE")
    print("="*80)
    
    return comparison_df


if __name__ == "__main__":
    comparison_results = main()
