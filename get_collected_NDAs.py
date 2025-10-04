"""Extract unique NDA numbers from collected_data_final.xlsx to use as test set.

This module provides the NDA numbers that we want to test in our pipeline.
These are the NDAs that have known ANDA matches in the collected data.
"""

import pandas as pd


def get_nda_list(collected_data_path: str = "collected_data_final.xlsx") -> list:
    """Load collected data and extract unique NDA numbers.
    
    Args:
        collected_data_path: Path to collected_data_final.xlsx
        
    Returns:
        List of unique NDA numbers (as integers)
    """
    print(f"Loading collected data from {collected_data_path}...")
    df = pd.read_excel(collected_data_path)
    
    # Get unique NDAs
    nda_list = sorted(df['NDA'].dropna().unique().astype(int).tolist())
    
    print(f"  Found {len(nda_list)} unique NDAs in collected data")
    print(f"  NDA range: {min(nda_list)} to {max(nda_list)}")
    
    return nda_list


def get_nda_approval_dates(collected_data_path: str = "collected_data_final.xlsx") -> dict:
    """Get NDA approval dates from collected data.
    
    Args:
        collected_data_path: Path to collected_data_final.xlsx
        
    Returns:
        Dictionary mapping NDA number to approval date
    """
    print(f"Loading NDA approval dates from {collected_data_path}...")
    df = pd.read_excel(collected_data_path)
    
    # Convert NDA Approval Date to datetime
    df['NDA Approval Date'] = pd.to_datetime(df['NDA Approval Date'], errors='coerce')
    
    # Get unique NDA to approval date mapping (take first occurrence)
    nda_dates = {}
    for nda in df['NDA'].dropna().unique():
        nda_rows = df[df['NDA'] == nda]
        if not nda_rows.empty:
            approval_date = nda_rows.iloc[0]['NDA Approval Date']
            if pd.notna(approval_date):
                nda_dates[int(nda)] = approval_date
    
    print(f"  Found approval dates for {len(nda_dates)} NDAs")
    
    return nda_dates


if __name__ == "__main__":
    # Test the functions
    nda_list = get_nda_list()
    print(f"\nFirst 10 NDAs: {nda_list[:10]}")
    print(f"Last 10 NDAs: {nda_list[-10:]}")
    
    nda_dates = get_nda_approval_dates()
    print(f"\nSample NDA dates:")
    for nda in list(nda_dates.keys())[:5]:
        print(f"  NDA {nda}: {nda_dates[nda].strftime('%Y-%m-%d')}")
