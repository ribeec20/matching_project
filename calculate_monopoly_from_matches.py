"""Calculate monopoly times from final_nda_anda_matches.txt and Orange Book data.

This script:
1. Reads the NDA-ANDA matches from the text file
2. Loads Orange Book data for approval dates
3. Calculates actual monopoly times (NDA approval to earliest ANDA)
4. Outputs results to CSV and console
"""

import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, List, Tuple

# File paths
MATCHES_FILE = "final_nda_anda_matches.txt"
ORANGE_BOOK_PATH = "OB - Products - Dec 2018.xlsx"
MAIN_TABLE_PATH = "Copy of Main Table - Dosage Strength.xlsx"
OUTPUT_CSV = "monopoly_times_from_matches.csv"


def parse_matches_file(filename: str) -> Dict[str, List[str]]:
    """Parse the final_nda_anda_matches.txt file.
    
    Args:
        filename: Path to the matches text file
        
    Returns:
        Dictionary mapping NDA number to list of ANDA numbers
    """
    nda_anda_map = {}
    
    with open(filename, 'r', encoding='utf-8') as f:
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
                    nda_num = parts[0].replace('NDA', '').strip()
                    anda_list_str = parts[1].strip()
                    
                    # Split by comma and get unique ANDAs
                    andas = [anda.strip() for anda in anda_list_str.split(',')]
                    unique_andas = sorted(set(andas))
                    
                    nda_anda_map[nda_num] = unique_andas
    
    return nda_anda_map


def load_orange_book_data(filepath: str) -> pd.DataFrame:
    """Load and preprocess Orange Book data.
    
    Args:
        filepath: Path to Orange Book Excel file
        
    Returns:
        DataFrame with Orange Book data
    """
    print(f"Loading Orange Book from {filepath}...")
    df = pd.read_excel(filepath)
    
    # Convert approval dates
    df['Approval_Date'] = pd.to_datetime(df['Approval_Date'], errors='coerce')
    
    return df


def load_main_table_data(filepath: str) -> pd.DataFrame:
    """Load main table for NDA information.
    
    Args:
        filepath: Path to main table Excel file
        
    Returns:
        DataFrame with main table data
    """
    print(f"Loading main table from {filepath}...")
    df = pd.read_excel(filepath)
    
    # Convert approval dates
    df['Approval_Date'] = pd.to_datetime(df['Approval_Date'], errors='coerce')
    
    return df


def get_nda_info(nda_num: str, main_table: pd.DataFrame, orange_book: pd.DataFrame) -> Dict:
    """Get NDA information including approval date and MMT.
    
    Args:
        nda_num: NDA application number
        main_table: Main table DataFrame
        orange_book: Orange Book DataFrame
        
    Returns:
        Dictionary with NDA info
    """
    nda_info = {
        'nda_approval_date': None,
        'mmt_years': None,
        'ingredient': None,
        'applicant': None
    }
    
    # Try main table first
    main_match = main_table[main_table['Appl_No'] == int(nda_num)]
    if not main_match.empty:
        row = main_match.iloc[0]
        nda_info['nda_approval_date'] = row.get('Approval_Date')
        nda_info['mmt_years'] = row.get('MMT_Years')
        nda_info['ingredient'] = row.get('Ingredient')
        nda_info['applicant'] = row.get('Applicant')
    
    # Try Orange Book if not in main table
    if nda_info['nda_approval_date'] is None or pd.isna(nda_info['nda_approval_date']):
        ob_match = orange_book[(orange_book['Appl_Type'] == 'N') & 
                               (orange_book['Appl_No'] == int(nda_num))]
        if not ob_match.empty:
            # Get earliest approval date for this NDA
            nda_info['nda_approval_date'] = ob_match['Approval_Date'].min()
            if nda_info['ingredient'] is None:
                nda_info['ingredient'] = ob_match.iloc[0].get('Ingredient')
            if nda_info['applicant'] is None:
                nda_info['applicant'] = ob_match.iloc[0].get('Applicant')
    
    return nda_info


def get_anda_approval_dates(anda_numbers: List[str], orange_book: pd.DataFrame) -> Dict[str, datetime]:
    """Get approval dates for a list of ANDAs.
    
    Args:
        anda_numbers: List of ANDA numbers
        orange_book: Orange Book DataFrame
        
    Returns:
        Dictionary mapping ANDA number to approval date
    """
    anda_dates = {}
    
    for anda_num in anda_numbers:
        try:
            anda_int = int(anda_num)
            anda_match = orange_book[(orange_book['Appl_Type'] == 'A') & 
                                     (orange_book['Appl_No'] == anda_int)]
            
            if not anda_match.empty:
                # Get earliest approval date for this ANDA
                anda_dates[anda_num] = anda_match['Approval_Date'].min()
        except ValueError:
            continue
    
    return anda_dates


def calculate_monopoly_times(nda_anda_map: Dict[str, List[str]], 
                            main_table: pd.DataFrame,
                            orange_book: pd.DataFrame) -> pd.DataFrame:
    """Calculate monopoly times for all NDA-ANDA matches.
    
    Args:
        nda_anda_map: Dictionary mapping NDA to list of ANDAs
        main_table: Main table DataFrame
        orange_book: Orange Book DataFrame
        
    Returns:
        DataFrame with monopoly time calculations
    """
    results = []
    
    print(f"\nCalculating monopoly times for {len(nda_anda_map)} NDAs...")
    
    for nda_num, anda_list in nda_anda_map.items():
        # Get NDA information
        nda_info = get_nda_info(nda_num, main_table, orange_book)
        
        if nda_info['nda_approval_date'] is None or pd.isna(nda_info['nda_approval_date']):
            print(f"⚠️  NDA {nda_num}: No approval date found")
            continue
        
        # Get ANDA approval dates
        anda_dates = get_anda_approval_dates(anda_list, orange_book)
        
        if not anda_dates:
            print(f"⚠️  NDA {nda_num}: No ANDA approval dates found")
            continue
        
        # Filter ANDAs approved after NDA
        valid_anda_dates = {
            anda: date for anda, date in anda_dates.items()
            if pd.notna(date) and date > nda_info['nda_approval_date']
        }
        
        if not valid_anda_dates:
            print(f"⚠️  NDA {nda_num}: No ANDAs approved after NDA")
            continue
        
        # Find earliest ANDA
        earliest_anda = min(valid_anda_dates.items(), key=lambda x: x[1])
        earliest_anda_num, earliest_anda_date = earliest_anda
        
        # Calculate monopoly time
        monopoly_days = (earliest_anda_date - nda_info['nda_approval_date']).days
        monopoly_years = monopoly_days / 365.25
        
        # Check if shorter than granted
        granted_years = nda_info['mmt_years'] if nda_info['mmt_years'] is not None else np.nan
        shorter_than_granted = monopoly_years < granted_years if pd.notna(granted_years) else np.nan
        
        results.append({
            'NDA_Appl_No': nda_num,
            'NDA_Ingredient': nda_info['ingredient'],
            'NDA_Applicant': nda_info['applicant'],
            'NDA_Approval_Date': nda_info['nda_approval_date'].strftime('%Y-%m-%d') if pd.notna(nda_info['nda_approval_date']) else None,
            'Granted_MMT_Years': granted_years,
            'Num_Matching_ANDAs': len(anda_list),
            'Num_Valid_ANDAs': len(valid_anda_dates),
            'Earliest_ANDA_Number': earliest_anda_num,
            'Earliest_ANDA_Date': earliest_anda_date.strftime('%Y-%m-%d') if pd.notna(earliest_anda_date) else None,
            'Actual_Monopoly_Days': monopoly_days,
            'Actual_Monopoly_Years': round(monopoly_years, 2),
            'Shorter_Than_Granted': shorter_than_granted,
            'Difference_Years': round(monopoly_years - granted_years, 2) if pd.notna(granted_years) else np.nan,
            'All_ANDAs': ' | '.join(anda_list)
        })
    
    return pd.DataFrame(results)


def main():
    """Main execution function."""
    print("=" * 80)
    print("MONOPOLY TIME CALCULATOR FROM MATCHES FILE")
    print("=" * 80)
    print()
    
    # Parse matches file
    print(f"1. Parsing {MATCHES_FILE}...")
    nda_anda_map = parse_matches_file(MATCHES_FILE)
    print(f"   Found {len(nda_anda_map)} NDAs with ANDA matches")
    print()
    
    # Load data
    print("2. Loading data files...")
    orange_book = load_orange_book_data(ORANGE_BOOK_PATH)
    main_table = load_main_table_data(MAIN_TABLE_PATH)
    print(f"   Orange Book: {len(orange_book)} records")
    print(f"   Main Table: {len(main_table)} records")
    print()
    
    # Calculate monopoly times
    print("3. Calculating monopoly times...")
    monopoly_df = calculate_monopoly_times(nda_anda_map, main_table, orange_book)
    print()
    
    # Summary statistics
    print("=" * 80)
    print("SUMMARY STATISTICS")
    print("=" * 80)
    print(f"Total NDAs processed: {len(nda_anda_map)}")
    print(f"NDAs with calculated monopoly times: {len(monopoly_df)}")
    print()
    
    if not monopoly_df.empty:
        print(f"Actual Monopoly Years:")
        print(f"  Min: {monopoly_df['Actual_Monopoly_Years'].min():.2f} years")
        print(f"  Max: {monopoly_df['Actual_Monopoly_Years'].max():.2f} years")
        print(f"  Mean: {monopoly_df['Actual_Monopoly_Years'].mean():.2f} years")
        print(f"  Median: {monopoly_df['Actual_Monopoly_Years'].median():.2f} years")
        print()
        
        if 'Shorter_Than_Granted' in monopoly_df.columns:
            shorter_count = monopoly_df['Shorter_Than_Granted'].sum()
            total_with_granted = monopoly_df['Shorter_Than_Granted'].notna().sum()
            if total_with_granted > 0:
                pct = 100 * shorter_count / total_with_granted
                print(f"NDAs with shorter actual monopoly: {shorter_count}/{total_with_granted} ({pct:.1f}%)")
                print()
    
    # Save to CSV
    print(f"4. Saving results to {OUTPUT_CSV}...")
    monopoly_df.to_csv(OUTPUT_CSV, index=False)
    print(f"   ✓ Saved {len(monopoly_df)} records")
    print()
    
    # Display first 10 results
    print("=" * 80)
    print("FIRST 10 RESULTS")
    print("=" * 80)
    display_cols = ['NDA_Appl_No', 'NDA_Ingredient', 'Granted_MMT_Years', 
                   'Actual_Monopoly_Years', 'Difference_Years', 'Num_Valid_ANDAs']
    print(monopoly_df[display_cols].head(10).to_string(index=False))
    print()
    
    print("=" * 80)
    print(f"✓ Complete! Results saved to {OUTPUT_CSV}")
    print("=" * 80)
    
    return monopoly_df


if __name__ == "__main__":
    results = main()
