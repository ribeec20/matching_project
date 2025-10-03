"""
Quick test script to extract data from a few sample FDA approval letter PDFs.
"""

from extract_from_pdf import FDAApprovalLetterExtractor, load_urls_from_analysis_file
import pandas as pd

def quick_test_extraction(num_urls: int = 10):
    """Test PDF extraction with a small number of URLs."""
    
    # Load URLs from your largest analysis file
    analysis_file = 'anda_analysis_after_2000_1000.txt'
    all_urls = load_urls_from_analysis_file(analysis_file)
    
    if not all_urls:
        print("No URLs found in analysis file")
        return
    
    # Take first few URLs for testing
    test_urls = all_urls[:num_urls]
    
    print(f"Testing PDF extraction with {len(test_urls)} URLs...")
    
    # Process URLs
    extractor = FDAApprovalLetterExtractor()
    results_df = extractor.process_url_list(test_urls, f'fda_test_{num_urls}_pdfs.csv')
    
    # Display results
    print(f"\n{'='*80}")
    print("EXTRACTION RESULTS SUMMARY")
    print(f"{'='*80}")
    
    successful = len(results_df[results_df['status'] == 'success'])
    failed = len(results_df[results_df['status'] != 'success'])
    
    print(f"✅ Successfully processed: {successful}")
    print(f"❌ Failed: {failed}")
    
    if successful > 0:
        success_df = results_df[results_df['status'] == 'success']
        
        print(f"\n📊 EXTRACTED DATA SAMPLE:")
        print("-" * 80)
        
        columns_to_show = ['drug_name', 'dosage_form', 'strengths', 'reference_drug', 'reference_company', 'applicant_company']
        sample_df = success_df[columns_to_show].head(5)
        
        for idx, row in sample_df.iterrows():
            print(f"\n[{idx+1}] {row['drug_name']} ({row['dosage_form']})")
            print(f"    Strengths: {row['strengths']}")
            print(f"    Reference: {row['reference_drug']} by {row['reference_company']}")
            print(f"    Applicant: {row['applicant_company']}")
        
        print(f"\n📁 Full results saved to: fda_test_{num_urls}_pdfs.csv")
        
        # Show data quality metrics
        print(f"\n📈 DATA QUALITY METRICS:")
        print("-" * 40)
        print(f"Drug names extracted: {len(success_df[success_df['drug_name'].notna()])}/{successful} ({len(success_df[success_df['drug_name'].notna()])/successful*100:.1f}%)")
        print(f"Reference companies: {len(success_df[success_df['reference_company'].notna()])}/{successful} ({len(success_df[success_df['reference_company'].notna()])/successful*100:.1f}%)")
        print(f"Applicant companies: {len(success_df[success_df['applicant_company'].notna()])}/{successful} ({len(success_df[success_df['applicant_company'].notna()])/successful*100:.1f}%)")
        print(f"Approval dates: {len(success_df[success_df['approval_date'].notna()])}/{successful} ({len(success_df[success_df['approval_date'].notna()])/successful*100:.1f}%)")

if __name__ == "__main__":
    # Test with 10 URLs first
    quick_test_extraction(10)