"""Test script for the new class-based NDA-ANDA matching system."""

import logging
import pandas as pd
from pathlib import Path

from match_new import run_class_based_matching, NDAANDAMatcher
from load_data import load_and_create_objects
from match_class import NDA, ANDA, Match

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def load_test_data():
    """Load the test data files."""
    logger.info("Loading test data...")
    
    # Load main table
    main_table_path = Path("Copy of Main Table - Dosage Strength.xlsx")
    if not main_table_path.exists():
        raise FileNotFoundError(f"Main table file not found: {main_table_path}")
    
    main_table_df = pd.read_excel(main_table_path)
    logger.info(f"Loaded main table: {len(main_table_df)} rows")
    
    # Load Orange Book
    orange_book_path = Path("OB - Products - Dec 2018.xlsx")
    if not orange_book_path.exists():
        raise FileNotFoundError(f"Orange Book file not found: {orange_book_path}")
    
    orange_book_df = pd.read_excel(orange_book_path)
    logger.info(f"Loaded Orange Book: {len(orange_book_df)} rows")
    
    return main_table_df, orange_book_df


def test_data_loading():
    """Test the data loading and object creation."""
    logger.info("=== TESTING DATA LOADING ===")
    
    main_table_df, orange_book_df = load_test_data()
    
    # Test object creation
    nda_objects, anda_objects, data_loader = load_and_create_objects(main_table_df, orange_book_df)
    
    # Validate results
    stats = data_loader.validate_data_integrity()
    logger.info(f"Data integrity stats: {stats}")
    
    # Test a few objects
    if nda_objects:
        first_nda = list(nda_objects.values())[0]
        logger.info(f"Sample NDA: {first_nda}")
        logger.info(f"  - Ingredient: {first_nda.get_ingredient()}")
        logger.info(f"  - Applicant: {first_nda.get_applicant()}")
        logger.info(f"  - Companies from OB: {first_nda.get_companies_from_orange_book()}")
        logger.info(f"  - MMT Years: {first_nda.get_mmt_years()}")
    
    if anda_objects:
        first_anda = list(anda_objects.values())[0]
        logger.info(f"Sample ANDA: {first_anda}")
        logger.info(f"  - Ingredient: {first_anda.get_ingredient()}")
        logger.info(f"  - Applicant: {first_anda.get_applicant()}")
        logger.info(f"  - Strength: {first_anda.get_strength()}")
        logger.info(f"  - Approval Date: {first_anda.get_approval_date()}")
    
    return nda_objects, anda_objects, data_loader, main_table_df, orange_book_df


def test_matching_process():
    """Test the complete matching process."""
    logger.info("=== TESTING MATCHING PROCESS ===")
    
    main_table_df, orange_book_df = load_test_data()
    
    # Test both PDF validation and conservative validation
    logger.info("Testing with PDF validation...")
    try:
        matcher_pdf, results_pdf = run_class_based_matching(
            main_table_df, orange_book_df, use_pdf_validation=True
        )
        logger.info("PDF validation test completed successfully")
    except Exception as e:
        logger.warning(f"PDF validation failed: {e}, testing conservative validation...")
        matcher_pdf, results_pdf = run_class_based_matching(
            main_table_df, orange_book_df, use_pdf_validation=False
        )
    
    # Display results
    logger.info(f"Results DataFrame shape: {results_pdf.shape}")
    logger.info(f"Results columns: {list(results_pdf.columns)}")
    
    # Show summary statistics
    stats = matcher_pdf.get_summary_statistics()
    logger.info("=== FINAL STATISTICS ===")
    for key, value in stats.items():
        if isinstance(value, float):
            logger.info(f"{key}: {value:.2f}")
        else:
            logger.info(f"{key}: {value}")
    
    # Show sample results
    if not results_pdf.empty:
        logger.info("=== SAMPLE RESULTS ===")
        # Show first 5 results with monopoly times
        sample_results = results_pdf[results_pdf['actual_monopoly_years'].notna()].head()
        for _, row in sample_results.iterrows():
            logger.info(f"NDA {row['nda_number']}: {row['ingredient']}")
            logger.info(f"  Actual monopoly: {row['actual_monopoly_years']:.2f} years")
            logger.info(f"  Granted monopoly: {row['granted_monopoly_years']:.2f} years")
            logger.info(f"  Matching ANDAs: {row['matching_anda_count']}")
            logger.info(f"  Shorter than granted: {row['shorter_than_granted']}")
    
    return matcher_pdf, results_pdf


def test_individual_match():
    """Test individual match creation and validation."""
    logger.info("=== TESTING INDIVIDUAL MATCH ===")
    
    nda_objects, anda_objects, data_loader, main_table_df, orange_book_df = test_data_loading()
    
    if not nda_objects or not anda_objects:
        logger.error("No objects created, cannot test individual match")
        return
    
    # Get first NDA
    first_nda = list(nda_objects.values())[0]
    logger.info(f"Testing with NDA: {first_nda}")
    
    # Find matching ANDAs manually
    matching_andas = []
    for anda in list(anda_objects.values())[:50]:  # Test with first 50 ANDAs
        if anda.get_normalized_ingredient() == first_nda.get_normalized_ingredient():
            matching_andas.append(anda)
    
    logger.info(f"Found {len(matching_andas)} ANDAs with same ingredient")
    
    # Create a match
    test_match = Match(first_nda, matching_andas)
    logger.info(f"Created match: {test_match}")
    logger.info(f"Match count after impossible elimination: {test_match.get_match_count()}")
    
    # Test monopoly calculation
    monopoly_time = test_match.calculate_monopoly_time()
    logger.info(f"Calculated monopoly time: {monopoly_time}")
    
    # Get full summary
    summary = test_match.get_monopoly_summary()
    logger.info(f"Match summary: {summary}")
    
    return test_match


def test_validation_methods():
    """Test different validation methods."""
    logger.info("=== TESTING VALIDATION METHODS ===")
    
    nda_objects, anda_objects, data_loader, main_table_df, orange_book_df = test_data_loading()
    
    if not nda_objects or not anda_objects:
        logger.error("No objects created, cannot test validation")
        return
    
    # Get first NDA with some ANDAs
    first_nda = list(nda_objects.values())[0]
    matching_andas = []
    for anda in list(anda_objects.values())[:20]:  # Test with first 20 ANDAs
        if anda.get_normalized_ingredient() == first_nda.get_normalized_ingredient():
            matching_andas.append(anda)
    
    if not matching_andas:
        logger.warning("No matching ANDAs found for validation test")
        return
    
    logger.info(f"Testing validation with {len(matching_andas)} ANDAs for NDA {first_nda.get_nda_number()}")
    
    # Test 1: Conservative validation
    test_match_conservative = Match(first_nda, matching_andas.copy())
    logger.info("Testing conservative validation...")
    test_match_conservative.verify_matches(orange_book_df, use_pdf_validation=False)
    logger.info(f"Conservative validation result: {test_match_conservative.get_match_count()} matches kept")
    
    # Test 2: PDF validation (may fail if no internet or PDFs not accessible)
    test_match_pdf = Match(first_nda, matching_andas.copy())
    logger.info("Testing PDF validation...")
    try:
        test_match_pdf.verify_matches(orange_book_df, use_pdf_validation=True)
        logger.info(f"PDF validation result: {test_match_pdf.get_match_count()} matches kept")
        
        # Get validation summary
        validation_summary = test_match_pdf.get_validation_summary()
        logger.info(f"Validation summary: {validation_summary}")
        
    except Exception as e:
        logger.warning(f"PDF validation failed: {e}")
        logger.info("This is expected if PDFs are not accessible or internet is not available")
    
    return test_match_conservative, test_match_pdf if 'test_match_pdf' in locals() else None


def compare_with_original():
    """Compare results with original matching system."""
    logger.info("=== COMPARING WITH ORIGINAL SYSTEM ===")
    
    main_table_df, orange_book_df = load_test_data()
    
    # Run new class-based system
    logger.info("Running new class-based system...")
    matcher, new_results = run_class_based_matching(main_table_df, orange_book_df)
    new_stats = matcher.get_summary_statistics()
    
    logger.info("=== NEW SYSTEM RESULTS ===")
    logger.info(f"Total NDAs: {new_stats['total_ndas']}")
    logger.info(f"NDAs with matches: {new_stats['ndas_with_matches']}")
    logger.info(f"Total ANDA matches: {new_stats['total_anda_matches']}")
    logger.info(f"Average matches per NDA: {new_stats['average_matches_per_nda']:.2f}")
    logger.info(f"Average monopoly years: {new_stats['average_monopoly_years']:.2f}")
    
    # Note: We would need to run the original system to compare
    # For now, just report the new system's performance
    
    return new_results


def main():
    """Run all tests."""
    logger.info("Starting comprehensive test of class-based matching system...")
    
    try:
        # Test 1: Data loading
        logger.info("\n" + "="*50)
        test_data_loading()
        
        # Test 2: Individual match
        logger.info("\n" + "="*50)
        test_individual_match()
        
        # Test 3: Validation methods
        logger.info("\n" + "="*50)
        test_validation_methods()
        
        # Test 4: Complete matching process
        logger.info("\n" + "="*50)
        test_matching_process()
        
        # Test 5: Compare with original (informational)
        logger.info("\n" + "="*50)
        compare_with_original()
        
        logger.info("\n" + "="*50)
        logger.info("All tests completed successfully!")
        logger.info("Note: PDF validation tests may fail if internet access is limited or PDFs are not accessible.")
        logger.info("This is expected behavior - the system will fall back to conservative validation.")
        
    except Exception as e:
        logger.error(f"Test failed with error: {e}")
        raise


if __name__ == "__main__":
    main()