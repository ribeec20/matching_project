"""Test script for the new company validation functionality."""

import pandas as pd
from preprocess import preprocess_data
from match import match_ndas_to_andas
from postprocess import nda_anda_company_validation

# File paths
MAIN_TABLE_PATH = "Copy of Main Table - Dosage Strength.xlsx"
ORANGE_BOOK_PATH = "OB - Products - Dec 2018.xlsx"

def test_company_validation():
    """Test the company validation process with a small sample."""
    print("Testing NDA-ANDA company validation...")
    print("=" * 50)
    
    # Step 1: Load and preprocess data
    print("1. Loading data...")
    main_table_clean, orange_book_clean = preprocess_data(MAIN_TABLE_PATH, ORANGE_BOOK_PATH)
    print(f"   Loaded {len(main_table_clean)} NDA records")
    print(f"   Loaded {len(orange_book_clean)} Orange Book records")
    
    # Step 2: Run matching
    print("2. Running NDA-ANDA matching...")
    match_data = match_ndas_to_andas(main_table_clean, orange_book_clean)
    print(f"   Found {len(match_data.anda_matches)} total matches")
    
    # Step 3: Test company validation with limited sample
    print("3. Testing company validation (limited to 3 ANDAs)...")
    validated_matches, rejected_matches, validation_details = nda_anda_company_validation(
        match_data, 
        orange_book_clean,
        max_andas_to_process=3  # Limit for testing
    )
    
    # Step 4: Display results
    print("\n4. Validation Results:")
    print(f"   Total matches processed: {validation_details['total_matches_processed']}")
    print(f"   Validated matches: {validation_details['validated_count']}")
    print(f"   Rejected matches: {validation_details['rejected_count']}")
    
    if len(validated_matches) > 0:
        print("\nValidated Matches (sample):")
        display_cols = ['NDA_Appl_No', 'ANDA_Appl_No', 'NDA_Companies', 'Matched_Company', 'Company_Match_Found']
        available_cols = [col for col in display_cols if col in validated_matches.columns]
        print(validated_matches[available_cols].head())
    
    if len(rejected_matches) > 0:
        print("\nRejected Matches (sample):")
        display_cols = ['NDA_Appl_No', 'ANDA_Appl_No', 'NDA_Companies', 'Company_Match_Found']
        available_cols = [col for col in display_cols if col in rejected_matches.columns]
        print(rejected_matches[available_cols].head())
    
    # Step 5: Show some reference text examples
    print("\nCompany Reference Text Examples:")
    for anda_num, ref_text in list(validation_details['company_references'].items())[:2]:
        print(f"\nANDA {anda_num}:")
        if ref_text:
            # Truncate long text for display
            display_text = ref_text[:200] + "..." if len(ref_text) > 200 else ref_text
            print(f"   {display_text}")
        else:
            print("   No reference text found")
    
    print("\nTest completed successfully!")
    return validated_matches, rejected_matches, validation_details

if __name__ == "__main__":
    validated_matches, rejected_matches, validation_details = test_company_validation()