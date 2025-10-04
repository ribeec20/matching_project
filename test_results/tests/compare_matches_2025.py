"""Compare NDA-ANDA matches from 2025 Orange Book pipeline with collected_data_final.xlsx.

This script compares:
1. Which NDAs have matches in both datasets
2. The earliest ANDA match for each NDA
3. The calculated monopoly time differences
4. Matches found by pipeline but not in collected data (and vice versa)
5. Top 5 earliest ANDA analysis

This is the 2025 version that uses results from dosage_2025.py pipeline.
"""

import pandas as pd
import numpy as np
from datetime import datetime

# File paths
COLLECTED_DATA_PATH = "collected_data_final.xlsx"
PIPELINE_MATCHES_PATH = "final_nda_anda_matches_2025.txt"
SUBMISSIONS_PATH = "txts/OB txts/Submissions.txt"
OUTPUT_COMPARISON_PATH = "match_comparison_report_2025.csv"
OUTPUT_TOP5_COMPARISON_PATH = "top5_anda_comparison_2025.txt"


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
    """Parse final_nda_anda_matches_2025.txt to extract NDA-ANDA mappings.
    
    Args:
        filepath: Path to final_nda_anda_matches_2025.txt
        
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


def load_submissions_data(filepath: str) -> pd.DataFrame:
    """Load Submissions.txt to get approval dates for NDAs and ANDAs.
    
    Args:
        filepath: Path to Submissions.txt
        
    Returns:
        DataFrame with application numbers and approval dates
    """
    print(f"\nLoading submission dates from {filepath}...")
    df = pd.read_csv(filepath, sep='\t', dtype={'ApplNo': str}, encoding='latin-1')
    
    # Convert to proper format
    df['Appl_No'] = df['ApplNo'].astype(int)
    df['Approval_Date'] = pd.to_datetime(df['SubmissionStatusDate'], errors='coerce')
    
    # Filter to only approved submissions
    df = df[df['SubmissionStatus'] == 'AP'].copy()
    
    # Get the earliest approval date for each application
    earliest_approvals = df.groupby('Appl_No')['Approval_Date'].min().reset_index()
    
    print(f"  Loaded approval dates for {len(earliest_approvals)} applications")
    
    return earliest_approvals


def calculate_monopoly_times_from_matches(pipeline_map: dict, submissions_df: pd.DataFrame) -> pd.DataFrame:
    """Calculate monopoly times from pipeline matches using submission dates.
    
    Args:
        pipeline_map: Dictionary of NDA -> list of ANDAs from pipeline
        submissions_df: DataFrame with approval dates from Submissions.txt
        
    Returns:
        DataFrame with NDA, earliest ANDA, and monopoly time calculations
    """
    print("\nCalculating monopoly times from pipeline matches...")
    
    # Create a lookup dictionary for approval dates
    approval_dates = dict(zip(submissions_df['Appl_No'], submissions_df['Approval_Date']))
    
    monopoly_records = []
    
    for nda_num, anda_list in pipeline_map.items():
        # Get NDA approval date
        nda_approval = approval_dates.get(nda_num)
        
        if nda_approval is None or pd.isna(nda_approval):
            continue
        
        # Get approval dates for all ANDAs
        anda_dates = []
        for anda_num in anda_list:
            anda_approval = approval_dates.get(anda_num)
            if anda_approval is not None and pd.notna(anda_approval):
                monopoly_days = (anda_approval - nda_approval).days
                anda_dates.append((anda_num, anda_approval, monopoly_days))
        
        if not anda_dates:
            continue
        
        # Find earliest ANDA (minimum monopoly time)
        anda_dates.sort(key=lambda x: x[2])  # Sort by monopoly days
        earliest_anda_num, earliest_anda_date, monopoly_days = anda_dates[0]
        
        monopoly_records.append({
            'NDA_Appl_No': nda_num,
            'NDA_Approval_Date': nda_approval,
            'Earliest_ANDA_Number': earliest_anda_num,
            'Earliest_ANDA_Date': earliest_anda_date,
            'Actual_Monopoly_Days': monopoly_days,
            'Actual_Monopoly_Years': monopoly_days / 365.25
        })
    
    monopoly_df = pd.DataFrame(monopoly_records)
    print(f"  Calculated monopoly times for {len(monopoly_df)} NDAs")
    
    return monopoly_df


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
    
    # Find ANDA with MINIMUM monopoly period per NDA
    shortest_monopoly = valid_data.loc[
        valid_data.groupby('NDA')['Length of Monopoly Period (Days)'].idxmin()
    ]
    
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
    print("COMPARISON SUMMARY (2025 Orange Book)")
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
        # Earliest ANDA agreement
        same_earliest = both_datasets[both_datasets['Earliest_ANDA_Match'] == True]
        different_earliest = both_datasets[both_datasets['Earliest_ANDA_Match'] == False]
        
        print(f"\n  EARLIEST ANDA COMPARISON:")
        print(f"    [SAME] Same earliest ANDA: {len(same_earliest)} ({100*len(same_earliest)/len(both_datasets):.1f}%)")
        print(f"    [DIFF] Different earliest ANDA: {len(different_earliest)} ({100*len(different_earliest)/len(both_datasets):.1f}%)")
        
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
    output_lines.append("TOP 5 ANDA COMPARISON: CORRECT ANDA vs PIPELINE TOP 5 EARLIEST (2025 Orange Book)")
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
    output_lines.append("SUMMARY STATISTICS (2025 Orange Book)")
    output_lines.append("=" * 100)
    output_lines.append(f"")
    output_lines.append(f"Total NDAs in both datasets: {total}")
    output_lines.append(f"")
    if total > 0:
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
    print("NDA-ANDA MATCH COMPARISON (2025 Orange Book)")
    print("Comparing Pipeline Results vs Collected Data")
    print("="*80)
    
    # Load data
    collected_data = load_collected_data(COLLECTED_DATA_PATH)
    pipeline_map = parse_pipeline_matches(PIPELINE_MATCHES_PATH)
    
    # Load submission dates for calculating monopoly times
    submissions_df = load_submissions_data(SUBMISSIONS_PATH)
    
    # Calculate monopoly times from matches
    pipeline_monopoly = calculate_monopoly_times_from_matches(pipeline_map, submissions_df)
    
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
