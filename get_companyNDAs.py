"""Extract NDA numbers and company names from Applications.txt.

This module provides NDA-to-company mappings from the Orange Book Applications file.
This replaces the company information that was previously in the main table Excel file.
"""

import pandas as pd


def load_applications(applications_path: str = "txts/OB txts/Applications.txt") -> pd.DataFrame:
    """Load Applications.txt file.
    
    Args:
        applications_path: Path to Applications.txt
        
    Returns:
        DataFrame with application data
    """
    print(f"Loading applications from {applications_path}...")
    df = pd.read_csv(applications_path, sep='\t', dtype={'ApplNo': str})
    
    # Filter to only NDA applications
    nda_df = df[df['ApplType'] == 'NDA'].copy()
    
    print(f"  Loaded {len(nda_df)} NDA applications")
    print(f"  Unique sponsors: {nda_df['SponsorName'].nunique()}")
    
    return nda_df


def get_nda_company_map(applications_path: str = "txts/OB txts/Applications.txt") -> dict:
    """Get mapping of NDA number to company/sponsor name.
    
    Args:
        applications_path: Path to Applications.txt
        
    Returns:
        Dictionary mapping NDA number (int) to company name (str)
    """
    print(f"Creating NDA-to-company mapping from {applications_path}...")
    df = load_applications(applications_path)
    
    # Convert ApplNo to int and create mapping
    nda_company_map = {}
    for _, row in df.iterrows():
        try:
            nda_num = int(row['ApplNo'])
            company = row['SponsorName']
            if pd.notna(company):
                nda_company_map[nda_num] = company.strip()
        except (ValueError, TypeError):
            continue
    
    print(f"  Created mapping for {len(nda_company_map)} NDAs")
    
    return nda_company_map


def create_main_table_equivalent(applications_path: str = "txts/OB txts/Applications.txt",
                                 nda_list: list = None,
                                 nda_dates: dict = None) -> pd.DataFrame:
    """Create a DataFrame equivalent to the main table with NDA and company info.
    
    This creates a structure similar to main_table_clean from the original pipeline,
    but using only the Applications.txt file.
    
    Args:
        applications_path: Path to Applications.txt
        nda_list: Optional list of NDAs to filter to (from collected data)
        nda_dates: Optional dictionary of NDA -> approval date
        
    Returns:
        DataFrame with columns matching clean_main_table output
    """
    print(f"Creating main table equivalent from {applications_path}...")
    
    # Load applications
    df = load_applications(applications_path)
    
    # Convert to main table format with all required columns
    main_table = pd.DataFrame({
        'NDA_Appl_No': df['ApplNo'].astype(int),
        'Appl_No': df['ApplNo'],  # Keep as string for merge operations
        'Company': df['SponsorName'],
        'Ingredient': None,  # Not available from Applications.txt
        'Approval_Date': None,  # Will be filled from nda_dates if provided
        'Product_Count': None,  # Not available
        'Strength_Count': None,  # Not available
        'DF': None,  # Not available
        'Route': None,  # Not available
        'Strength': None,  # Not available
        'MMT': None,  # Not available
        'MMT_Years': None,  # Not available
    })
    
    # Fill in approval dates if provided
    if nda_dates is not None:
        main_table['Approval_Date'] = main_table['NDA_Appl_No'].map(nda_dates)
    
    # Filter to specific NDAs if provided
    if nda_list is not None:
        print(f"  Filtering to {len(nda_list)} NDAs from collected data...")
        main_table = main_table[main_table['NDA_Appl_No'].isin(nda_list)]
        print(f"  Filtered to {len(main_table)} NDA records")
    
    # Remove duplicates (keep first occurrence)
    main_table = main_table.drop_duplicates(subset=['NDA_Appl_No'])
    
    print(f"  Created main table with {len(main_table)} unique NDAs")
    
    return main_table


if __name__ == "__main__":
    # Test the functions
    nda_company_map = get_nda_company_map()
    
    print(f"\nSample NDA-Company mappings:")
    for nda in list(nda_company_map.keys())[:10]:
        print(f"  NDA {nda}: {nda_company_map[nda]}")
    
    # Create main table equivalent
    main_table = create_main_table_equivalent()
    print(f"\nMain table preview:")
    print(main_table.head(10))
