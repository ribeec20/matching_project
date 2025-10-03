"""Main analysis script for NDA-ANDA monopoly time analysis.

This script uses the new class-based system to:
1. Load and preprocess data from Excel files
2. Match NDA products to ANDA records using class-based matching
3. Apply PDF-based company validation
4. Generate monopoly time visualizations
"""

# pip installs (run once in your environment)
# pip install pandas numpy openpyxl matplotlib plotly

import numpy as np
import pandas as pd

# Import our class-based modular components
from preprocess import preprocess_data
from match_new import run_class_based_matching
from monopoly_time import plot_monopoly_scatter

# File paths for the data
MAIN_TABLE_PATH = "Copy of Main Table - Dosage Strength.xlsx"
ORANGE_BOOK_PATH = "OB - Products - Dec 2018.xlsx"

def main():
    """Main analysis pipeline using class-based system."""
    print("Starting NDA-ANDA monopoly time analysis with class-based system...")
    print("=" * 50)
    
    # Step 1: Load and preprocess data
    print("1. Loading and preprocessing data...")
    main_table_clean, orange_book_clean = preprocess_data(MAIN_TABLE_PATH, ORANGE_BOOK_PATH)
    print(f"   Loaded {len(main_table_clean)} NDA records from main table")
    print(f"   Loaded {len(orange_book_clean)} records from Orange Book")
    print()
    
    # Step 2: Run class-based NDA-ANDA matching with PDF validation
    print("2. Running class-based NDA-ANDA matching with PDF validation...")
    print("   Creating NDA and ANDA objects...")
    print("   Applying matching criteria (ingredient, dosage form, route, strength)...")
    print("   Running PDF-based company validation...")
    print("   Note: Using conservative validation - only rejecting matches with confirmed conflicts")
    
    # Run the complete class-based matching process with PDF validation
    matcher, results_df = run_class_based_matching(
        main_table_clean, 
        orange_book_clean,
        use_pdf_validation=True  # Enable PDF validation by default
    )
    
    print(f"   Created {len(matcher.nda_objects)} NDA objects")
    print(f"   Created {len(matcher.anda_objects)} ANDA objects") 
    print(f"   Generated {len(matcher.match_objects)} Match objects")
    print(f"   Results DataFrame has {len(results_df)} rows")
    print()
    
    # Step 3: Display summary statistics
    print("3. Generating summary statistics...")
    stats = matcher.get_summary_statistics()
    print("   === MATCHING SUMMARY ===")
    for key, value in stats.items():
        if isinstance(value, float):
            print(f"   {key}: {value:.2f}")
        else:
            print(f"   {key}: {value}")
    print()
    
    # Step 4: Create monopoly time visualizations
    print("4. Creating monopoly time visualizations...")
    if len(results_df) > 0:
        # Extract match objects for plotting
        plot_monopoly_scatter(matcher.match_objects, show=True)
        print("   Monopoly time visualizations generated successfully!")
    else:
        print("   No matches found for visualization")
    print()
    
    print("Analysis complete!")
    print("=" * 50)
    
    return matcher, results_df


if __name__ == "__main__":
    # Run the analysis
    matcher, results = main()