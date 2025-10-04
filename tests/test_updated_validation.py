"""Test the updated company validation functionality."""

import pandas as pd
from preprocess import preprocess_data
from match import match_ndas_to_andas
from postprocess import (
    nda_anda_company_validation,
    create_validated_match_data,
    calculate_nda_monopoly_times_with_validation
)
from monopoly_time import plot_monopoly_scatter

# File paths
MAIN_TABLE_PATH = "Copy of Main Table - Dosage Strength.xlsx"
ORANGE_BOOK_PATH = "OB - Products - Dec 2018.xlsx"

def test_updated_validation():
    """Test the updated company validation with 90% similarity and Main Table source."""
    print("Testing Updated NDA-ANDA Company Validation...")
    print("=" * 55)
    
    # Step 1: Load and preprocess data
    print("1. Loading data...")
    main_table_clean, orange_book_clean = preprocess_data(MAIN_TABLE_PATH, ORANGE_BOOK_PATH)
    print(f"   Loaded {len(main_table_clean)} NDA records")
    print(f"   Loaded {len(orange_book_clean)} Orange Book records")
    
    # Step 2: Run matching
    print("2. Running NDA-ANDA matching...")
    match_data = match_ndas_to_andas(main_table_clean, orange_book_clean)
    print(f"   Found {len(match_data.anda_matches)} total matches")
    
    # Step 3: Test updated company validation (limited sample)
    print("3. Testing updated company validation (limited to 5 ANDAs)...")
    validated_matches, rejected_matches, validation_details = nda_anda_company_validation(
        match_data, 
        orange_book_clean,
        main_table_clean,  # Now using main table as primary source
        max_andas_to_process=5
    )
    
    # Step 4: Display results
    print("\n4. Validation Results:")
    print(f"   Total matches processed: {validation_details['total_matches_processed']}")
    print(f"   Validated matches: {validation_details['validated_count']}")
    print(f"   Rejected matches: {validation_details['rejected_count']}")
    
    if len(validated_matches) > 0:
        print("\nValidated Matches (sample):")
        display_cols = ['NDA_Appl_No', 'ANDA_Appl_No', 'NDA_Companies', 'Matched_Company', 
                       'Company_Match_Similarity', 'Validation_Status']
        available_cols = [col for col in display_cols if col in validated_matches.columns]
        print(validated_matches[available_cols].head())
    
    # Step 5: Test monopoly time calculation with validation
    print("\n5. Testing monopoly time calculation with validation...")
    if len(validated_matches) > 0:
        # Create validated match data
        validated_match_data = create_validated_match_data(match_data, validated_matches)
        
        # Calculate monopoly times with validation status
        monopoly_times = calculate_nda_monopoly_times_with_validation(validated_match_data)
        
        print(f"   Calculated monopoly times for {len(monopoly_times)} NDAs")
        
        # Show validation status distribution
        if 'Validation_Status' in monopoly_times.columns:
            status_counts = monopoly_times['Validation_Status'].value_counts()
            print(f"   Validation status distribution:")
            for status, count in status_counts.items():
                print(f"     {status}: {count}")
        
        # Test visualization with validation colors
        print("\n6. Testing updated visualization...")
        fig = plot_monopoly_scatter(monopoly_times, show=False)
        print("   ✓ Visualization generated with validation status colors")
    
    print("\nUpdated validation test completed successfully!")
    return validated_matches, rejected_matches, validation_details

if __name__ == "__main__":
    validated_matches, rejected_matches, validation_details = test_updated_validation()