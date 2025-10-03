"""Test file for get_api_submissions function using ANDA objects.

This script follows the same flow as dosage.py and load_data.py to:
1. Load and preprocess data from Excel files
2. Create ANDA objects from Orange Book data
3. Select 20 random ANDAs for testing
4. Call get_api_submissions to get PDF URLs
5. Display results
"""

import pandas as pd
import random
from typing import List, Dict, Optional

# Import our class-based modular components
from preprocess import preprocess_data
from load_data import load_and_create_objects
from postprocess import get_api_submissions
from match_class import ANDA

# File paths for the data
MAIN_TABLE_PATH = "Copy of Main Table - Dosage Strength.xlsx"
ORANGE_BOOK_PATH = "OB - Products - Dec 2018.xlsx"


def select_random_andas(anda_objects: Dict[str, ANDA], count: int = 20) -> List[ANDA]:
    """Select random ANDA objects for testing.
    
    Args:
        anda_objects: Dictionary of ANDA objects
        count: Number of random ANDAs to select
        
    Returns:
        List of randomly selected ANDA objects
    """
    all_andas = list(anda_objects.values())
    
    # Select random ANDAs (or all if fewer than requested)
    sample_size = min(count, len(all_andas))
    selected_andas = random.sample(all_andas, sample_size)
    
    print(f"Selected {len(selected_andas)} random ANDAs for testing:")
    for i, anda in enumerate(selected_andas, 1):
        print(f"  {i:2d}. ANDA {anda.get_anda_number()}: {anda.get_ingredient()} - {anda.get_applicant()}")
    
    return selected_andas


def display_api_results(pdf_urls: Dict[str, Optional[str]]) -> None:
    """Display the results of the API submission search.
    
    Args:
        pdf_urls: Dictionary mapping ANDA number to PDF URL (or None)
    """
    print("\n" + "=" * 80)
    print("API SUBMISSION SEARCH RESULTS")
    print("=" * 80)
    
    found_urls = []
    not_found = []
    
    for anda_number, pdf_url in pdf_urls.items():
        if pdf_url:
            found_urls.append((anda_number, pdf_url))
        else:
            not_found.append(anda_number)
    
    print(f"Total ANDAs searched: {len(pdf_urls)}")
    print(f"PDF URLs found: {len(found_urls)}")
    print(f"PDF URLs not found: {len(not_found)}")
    print(f"Success rate: {len(found_urls)/len(pdf_urls)*100:.1f}%")
    print()
    
    if found_urls:
        print("FOUND PDF URLs:")
        for anda_number, pdf_url in found_urls:
            print(f"  ANDA {anda_number}: {pdf_url}")
        print()
    
    if not_found:
        print("ANDAs WITHOUT PDF URLs:")
        for anda_number in not_found:
            print(f"  ANDA {anda_number}: No PDF found")
        print()


def test_api_submissions():
    """Main test function for get_api_submissions."""
    print("Testing get_api_submissions function with random ANDA objects...")
    print("=" * 60)
    
    # Step 1: Load and preprocess data (same as dosage.py)
    print("1. Loading and preprocessing data...")
    main_table_clean, orange_book_clean = preprocess_data(MAIN_TABLE_PATH, ORANGE_BOOK_PATH)
    print(f"   Loaded {len(main_table_clean)} NDA records from main table")
    print(f"   Loaded {len(orange_book_clean)} records from Orange Book")
    print()
    
    # Step 2: Create objects (same as load_data.py)
    print("2. Creating NDA and ANDA objects...")
    nda_objects, anda_objects, loader = load_and_create_objects(main_table_clean, orange_book_clean)
    print(f"   Created {len(nda_objects)} NDA objects")
    print(f"   Created {len(anda_objects)} ANDA objects")
    print()
    
    # Step 3: Select random ANDAs for testing
    print("3. Selecting random ANDAs for API testing...")
    random.seed(42)  # For reproducible results
    selected_andas = select_random_andas(anda_objects, count=20)
    print()
    
    # Step 4: Test get_api_submissions
    print("4. Calling get_api_submissions...")
    print("   Note: This will make API calls to FDA - please be patient...")
    try:
        pdf_urls = get_api_submissions(selected_andas)
        
        # Step 5: Display results
        display_api_results(pdf_urls)
        
        # Additional analysis
        print("DETAILED ANALYSIS:")
        print("-" * 40)
        
        # Check for URL patterns
        fda_urls = 0
        appletter_urls = 0
        
        for anda_number, pdf_url in pdf_urls.items():
            if pdf_url:
                if 'http://www.accessdata.fda.gov' in pdf_url:
                    fda_urls += 1
                if '/appletter/' in pdf_url:
                    appletter_urls += 1
        
        print(f"URLs containing 'http://www.accessdata.fda.gov': {fda_urls}")
        print(f"URLs containing '/appletter/': {appletter_urls}")
        print()
        
        # Check ANDAs by approval year (if available)
        year_analysis = {}
        for anda in selected_andas:
            approval_date = anda.get_approval_date()
            if approval_date:
                year = approval_date.year
                anda_number = anda.get_anda_number()
                has_pdf = pdf_urls.get(anda_number) is not None
                
                if year not in year_analysis:
                    year_analysis[year] = {'total': 0, 'with_pdf': 0}
                year_analysis[year]['total'] += 1
                if has_pdf:
                    year_analysis[year]['with_pdf'] += 1
        
        if year_analysis:
            print("PDF availability by approval year:")
            for year in sorted(year_analysis.keys()):
                stats = year_analysis[year]
                rate = stats['with_pdf'] / stats['total'] * 100
                print(f"  {year}: {stats['with_pdf']}/{stats['total']} ({rate:.1f}%)")
        
    except Exception as e:
        print(f"Error during API testing: {str(e)}")
        import traceback
        traceback.print_exc()
    
    print("\nAPI submission testing complete!")


if __name__ == "__main__":
    test_api_submissions()
