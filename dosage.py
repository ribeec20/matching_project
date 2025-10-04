"""Main analysis script for NDA-ANDA monopoly time analysis.

This script uses modular components to:
1. Load and preprocess data from Excel files
2. Match NDA products to ANDA records
3. Generate diagnostic summaries
4. Create monopoly time visualizations
"""

# pip installs (run once in your environment)
# pip install pandas numpy openpyxl matplotlib

import numpy as np
import pandas as pd

# Import our modular components
from preprocess import preprocess_data
from match import match_ndas_to_andas
from postprocess import (
    build_postprocess_outputs, 
    display_postprocess_summary,
    nda_anda_company_validation,
    create_validated_match_data,
    calculate_nda_monopoly_times_with_validation
)
from monopoly_time import plot_monopoly_scatter

# File paths for the data
MAIN_TABLE_PATH = "Copy of Main Table - Dosage Strength.xlsx"
ORANGE_BOOK_PATH = "OB - Products - Dec 2018.xlsx"

def main():
    """Main analysis pipeline."""
    print("Starting NDA-ANDA monopoly time analysis...")
    print("=" * 50)
    
    # Step 1: Load and preprocess data
    print("1. Loading and preprocessing data...")
    main_table_clean, orange_book_clean = preprocess_data(MAIN_TABLE_PATH, ORANGE_BOOK_PATH)
    print(f"   Loaded {len(main_table_clean)} NDA records from main table")
    print(f"   Loaded {len(orange_book_clean)} records from Orange Book")
    print()
    
    # Step 2: Run NDA-ANDA matching
    print("2. Running NDA-ANDA matching algorithm...")
    match_data = match_ndas_to_andas(main_table_clean, orange_book_clean)
    print(f"   Generated NDA summary with {len(match_data.nda_summary)} NDAs")
    print(f"   Found {len(match_data.anda_matches)} total NDA-ANDA matches")
    print()
    
    # Step 3: Run company validation on matches
    print("3. Running company validation using FDA approval letters...")
    print("   This will query the FDA API and extract company references from PDFs")
    print("   (This may take several minutes depending on the number of ANDAs)")
    print()
    
    validated_matches, rejected_matches, validation_details = nda_anda_company_validation(
        match_data=match_data,
        orange_book_clean=orange_book_clean,
        main_table_clean=main_table_clean,
        max_andas_to_process=None  # Set to a number (e.g., 50) to limit for testing
    )
    
    print(f"   Validation complete:")
    print(f"   - Validated matches: {len(validated_matches)}")
    print(f"   - Rejected matches: {len(rejected_matches)}")
    print(f"   - PDFs processed: {len(validation_details['anda_pdf_urls'])}")
    print()
    
    # Step 4: Create validated match data object
    print("4. Creating validated match dataset...")
    validated_match_data = create_validated_match_data(match_data, validated_matches)
    print(f"   Updated match data with {len(validated_match_data.anda_matches)} validated matches")
    print()
    
    # Step 5: Generate diagnostic outputs with validated data
    print("5. Generating diagnostic summaries and calculating monopoly times...")
    print("   (Using validated matches only - ANDAs filtered by date and company)")
    outputs = build_postprocess_outputs(validated_match_data)
    display_postprocess_summary(outputs)
    
    # Step 6: Create monopoly time visualization
    print("6. Creating monopoly time scatter plot...")
    plot_monopoly_scatter(outputs["nda_monopoly_times"], show=True)
    print("   Monopoly time scatter plot generated successfully!")
    print()
    
    # Step 7: Export results to text files
    print("7. Exporting results to text files...")
    export_nda_anda_matches(validated_match_data, filename="final_nda_anda_matches.txt")
    export_pdf_extraction_status(validation_details, filename="pdf_extraction_status.txt")
    print("   ✓ Exported final_nda_anda_matches.txt")
    print("   ✓ Exported pdf_extraction_status.txt")
    print()
    
    print("Analysis complete!")
    print("=" * 50)
    
    # Return both original and validated data for comparison
    return {
        'original_match_data': match_data,
        'validated_match_data': validated_match_data,
        'validated_matches': validated_matches,
        'rejected_matches': rejected_matches,
        'validation_details': validation_details,
        'outputs': outputs
    }


def export_nda_anda_matches(validated_match_data, filename="final_nda_anda_matches.txt"):
    """Export final NDA-ANDA matches to a text file.
    
    Format:
    NDA012345: ANDA067890, ANDA067891, ANDA067892
    NDA012346: ANDA067893
    
    Args:
        validated_match_data: MatchData object with validated matches
        filename: Output filename
    """
    with open(filename, 'w', encoding='utf-8') as f:
        # Write header
        f.write("=" * 80 + "\n")
        f.write("FINAL NDA-ANDA MATCHES (After Company Validation)\n")
        f.write("=" * 80 + "\n")
        f.write(f"Generated: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("\n")
        
        # Group ANDAs by NDA
        if validated_match_data.anda_matches.empty:
            f.write("No validated matches found.\n")
            return
        
        # Get unique NDAs and their matched ANDAs
        nda_anda_map = {}
        for _, row in validated_match_data.anda_matches.iterrows():
            nda_num = str(row['NDA_Appl_No'])
            anda_num = str(row['ANDA_Appl_No'])
            
            if nda_num not in nda_anda_map:
                nda_anda_map[nda_num] = []
            nda_anda_map[nda_num].append(anda_num)
        
        # Sort NDAs and write matches
        f.write(f"Total NDAs with matches: {len(nda_anda_map)}\n")
        f.write(f"Total validated ANDA matches: {len(validated_match_data.anda_matches)}\n")
        f.write("\n" + "-" * 80 + "\n\n")
        
        for nda_num in sorted(nda_anda_map.keys()):
            anda_list = sorted(nda_anda_map[nda_num])
            f.write(f"NDA{nda_num}: {', '.join(anda_list)}\n")


def export_pdf_extraction_status(validation_details, filename="pdf_extraction_status.txt"):
    """Export PDF extraction status to a text file.
    
    Shows which approval letters were successfully extracted and which failed.
    
    Args:
        validation_details: Dictionary with validation details including PDF URLs and company references
        filename: Output filename
    """
    with open(filename, 'w', encoding='utf-8') as f:
        # Write header
        f.write("=" * 80 + "\n")
        f.write("FDA APPROVAL LETTER PDF EXTRACTION STATUS\n")
        f.write("=" * 80 + "\n")
        f.write(f"Generated: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("\n")
        
        anda_pdf_urls = validation_details.get('anda_pdf_urls', {})
        company_references = validation_details.get('company_references', {})
        
        # Calculate statistics
        total_andas = len(anda_pdf_urls)
        pdfs_found = sum(1 for url in anda_pdf_urls.values() if url is not None)
        pdfs_extracted = sum(1 for ref in company_references.values() if ref is not None)
        
        f.write("SUMMARY\n")
        f.write("-" * 80 + "\n")
        f.write(f"Total ANDAs processed: {total_andas}\n")
        f.write(f"PDFs found via FDA API: {pdfs_found} ({100*pdfs_found/total_andas:.1f}%)\n")
        f.write(f"Company references extracted: {pdfs_extracted} ({100*pdfs_extracted/total_andas:.1f}%)\n")
        f.write(f"Failed extractions: {total_andas - pdfs_extracted}\n")
        f.write("\n")
        
        # Successful extractions
        f.write("=" * 80 + "\n")
        f.write("SUCCESSFUL EXTRACTIONS\n")
        f.write("=" * 80 + "\n\n")
        
        success_count = 0
        for anda_num in sorted(anda_pdf_urls.keys()):
            pdf_url = anda_pdf_urls[anda_num]
            company_ref = company_references.get(anda_num)
            
            if pdf_url and company_ref:
                success_count += 1
                f.write(f"[{success_count}] ANDA{anda_num}\n")
                f.write(f"    PDF URL: {pdf_url}\n")
                f.write(f"    Company Reference: {company_ref[:100]}...\n")  # First 100 chars
                f.write("\n")
        
        # Failed extractions
        f.write("=" * 80 + "\n")
        f.write("FAILED EXTRACTIONS\n")
        f.write("=" * 80 + "\n\n")
        
        failed_count = 0
        for anda_num in sorted(anda_pdf_urls.keys()):
            pdf_url = anda_pdf_urls[anda_num]
            company_ref = company_references.get(anda_num)
            
            if not pdf_url or not company_ref:
                failed_count += 1
                f.write(f"[{failed_count}] ANDA{anda_num}\n")
                
                if not pdf_url:
                    f.write(f"    Status: ✗ PDF not found via FDA API\n")
                elif not company_ref:
                    f.write(f"    Status: ✗ PDF found but company reference extraction failed\n")
                    f.write(f"    PDF URL: {pdf_url}\n")
                
                f.write("\n")


if __name__ == "__main__":
    # Run the analysis
    results = main()
    
    # Access results:
    # results['original_match_data'] - original matching before validation
    # results['validated_match_data'] - matches after company validation
    # results['validated_matches'] - DataFrame of validated matches
    # results['rejected_matches'] - DataFrame of rejected matches
    # results['validation_details'] - detailed validation information
    # results['outputs'] - diagnostic outputs (monopoly times, summaries, etc.)