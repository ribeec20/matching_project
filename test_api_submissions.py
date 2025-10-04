"""Test file for get_api_submissions function using ANDA data from Orange Book.

This script:
1. Loads Orange Book data
2. Extracts random ANDAs
3. Creates simple ANDA objects with required methods
4. Calls get_api_submissions to get PDF URLs from FDA API
5. Displays results
"""

import pandas as pd
import random
from typing import List, Dict, Optional
from datetime import datetime

# Import our modular components
from preprocess import preprocess_data
from postprocess import get_api_submissions

# File paths for the data
MAIN_TABLE_PATH = "Copy of Main Table - Dosage Strength.xlsx"
ORANGE_BOOK_PATH = "OB - Products - Dec 2018.xlsx"


class SimpleANDA:
    """Simple ANDA object with minimal interface for API testing."""
    
    def __init__(self, anda_number: str, approval_date: Optional[datetime] = None, 
                 ingredient: str = "", applicant: str = ""):
        self.anda_number = anda_number
        self.approval_date = approval_date
        self.ingredient = ingredient
        self.applicant = applicant
    
    def get_anda_number(self) -> str:
        return self.anda_number
    
    def get_approval_date(self) -> Optional[datetime]:
        return self.approval_date
    
    def get_ingredient(self) -> str:
        return self.ingredient
    
    def get_applicant(self) -> str:
        return self.applicant


def select_random_andas_from_orange_book(orange_book_clean: pd.DataFrame, count: int = 20) -> List[SimpleANDA]:
    """Select random ANDA records from Orange Book data.
    
    Args:
        orange_book_clean: Orange Book DataFrame
        count: Number of random ANDAs to select
        
    Returns:
        List of SimpleANDA objects
    """
    # Filter to ANDAs only
    anda_records = orange_book_clean[orange_book_clean['Appl_Type'] == 'A'].copy()
    
    # Get unique ANDAs
    unique_andas = anda_records['Appl_No'].unique()
    
    # Select random ANDAs
    sample_size = min(count, len(unique_andas))
    selected_anda_numbers = random.sample(list(unique_andas), sample_size)
    
    # Create SimpleANDA objects
    anda_objects = []
    for anda_num in selected_anda_numbers:
        # Get first record for this ANDA to extract info
        anda_row = anda_records[anda_records['Appl_No'] == anda_num].iloc[0]
        
        anda_obj = SimpleANDA(
            anda_number=anda_num,
            approval_date=anda_row.get('Approval_Date'),
            ingredient=anda_row.get('Ingredient', ''),
            applicant=anda_row.get('Applicant', '')
        )
        anda_objects.append(anda_obj)
    
    print(f"Selected {len(anda_objects)} random ANDAs for testing:")
    for i, anda in enumerate(anda_objects, 1):
        approval_date = anda.get_approval_date()
        if pd.notna(approval_date):
            if isinstance(approval_date, str):
                approval_year = approval_date[:4] if len(approval_date) >= 4 else 'Unknown'
            else:
                approval_year = approval_date.year
        else:
            approval_year = 'Unknown'
        
        ingredient = anda.get_ingredient()[:40] if anda.get_ingredient() else 'Unknown'
        print(f"  {i:2d}. ANDA {anda.get_anda_number()}: {ingredient:40s} ({approval_year}) - {anda.get_applicant()}")
    
    return anda_objects


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
    print("Testing get_api_submissions function with random ANDAs from Orange Book...")
    print("=" * 80)
    
    # Step 1: Load and preprocess data
    print("1. Loading and preprocessing data...")
    main_table_clean, orange_book_clean = preprocess_data(MAIN_TABLE_PATH, ORANGE_BOOK_PATH)
    print(f"   Loaded {len(main_table_clean)} NDA records from main table")
    print(f"   Loaded {len(orange_book_clean)} records from Orange Book")
    print()
    
    # Step 2: Select random ANDAs for testing
    print("2. Selecting random ANDAs for API testing...")
    random.seed(42)  # For reproducible results
    selected_andas = select_random_andas_from_orange_book(orange_book_clean, count=200)  # Testing with 200 ANDAs
    print()
    
    # Step 3: Test get_api_submissions
    print("3. Calling get_api_submissions with FDA API...")
    print("   Note: This will make API calls to FDA - please be patient...")
    print("   Each ANDA query has a 0.5 second delay for rate limiting")
    print()
    
    try:
        pdf_urls = get_api_submissions(selected_andas)
        
        # Step 4: Display results
        display_api_results(pdf_urls)
        
        # Additional analysis
        print("DETAILED ANALYSIS:")
        print("-" * 40)
        
        # Check for URL patterns
        fda_urls = 0
        appletter_urls = 0
        
        for anda_number, pdf_url in pdf_urls.items():
            if pdf_url:
                if 'http://www.accessdata.fda.gov' in pdf_url or 'https://www.accessdata.fda.gov' in pdf_url:
                    fda_urls += 1
                if '/appletter/' in pdf_url:
                    appletter_urls += 1
        
        print(f"URLs containing FDA domain: {fda_urls}")
        print(f"URLs containing '/appletter/': {appletter_urls}")
        print()
        
        # Check ANDAs by approval year (if available)
        year_analysis = {}
        for anda in selected_andas:
            approval_date = anda.get_approval_date()
            if pd.notna(approval_date):
                # Handle both string and datetime approval dates
                if isinstance(approval_date, str):
                    try:
                        year = int(approval_date[:4]) if len(approval_date) >= 4 else None
                    except (ValueError, TypeError):
                        year = None
                else:
                    year = approval_date.year if hasattr(approval_date, 'year') else None
                
                if year:
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
                rate = stats['with_pdf'] / stats['total'] * 100 if stats['total'] > 0 else 0
                print(f"  {year}: {stats['with_pdf']}/{stats['total']} ({rate:.1f}%)")
            print()
        
        # Show some example URLs if found
        found_urls = [(k, v) for k, v in pdf_urls.items() if v is not None]
        if found_urls:
            print("EXAMPLE PDF URLs FOUND:")
            for anda_num, url in found_urls[:5]:  # Show first 5
                print(f"  ANDA {anda_num}:")
                print(f"    {url}")
            if len(found_urls) > 5:
                print(f"  ... and {len(found_urls) - 5} more")
        
    except Exception as e:
        print(f"Error during API testing: {str(e)}")
        import traceback
        traceback.print_exc()
    
    print("\n" + "=" * 80)
    print("API submission testing complete!")


if __name__ == "__main__":
    test_api_submissions()
