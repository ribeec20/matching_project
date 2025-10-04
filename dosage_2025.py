"""Main analysis script for NDA-ANDA monopoly time analysis using 2025 Orange Book data.

This script uses the latest Orange Book Products.txt file and runs the same pipeline
as dosage.py, but with data sources from:
- Products.txt (Orange Book products)
- Applications.txt (NDA company/sponsor names)
- collected_data_final.xlsx (NDAs to test)

Pipeline steps:
1. Load NDAs from collected_data_final.xlsx
2. Load company info from Applications.txt
3. Load Orange Book products from Products.txt
4. Match NDA products to ANDA records
5. Generate diagnostic summaries
6. Create monopoly time visualizations
7. Export results
"""

import numpy as np
import pandas as pd

# Import our modular components
from match import match_ndas_to_andas
from postprocess import (
    build_postprocess_outputs, 
    display_postprocess_summary,
    nda_anda_company_validation,
    create_validated_match_data,
    calculate_nda_monopoly_times_with_validation
)
from monopoly_time import plot_monopoly_scatter

# Import new helper modules for 2025 data
from get_collected_NDAs import get_nda_list, get_nda_approval_dates
from get_companyNDAs import create_main_table_equivalent

# File paths for the 2025 data
PRODUCTS_PATH = "txts/OB txts/Products.txt"
APPLICATIONS_PATH = "txts/OB txts/Applications.txt"
COLLECTED_DATA_PATH = "collected_data_final.xlsx"


def load_products_txt(products_path: str) -> pd.DataFrame:
    """Load and preprocess Products.txt from Orange Book.
    
    This replaces the Excel-based Orange Book loading from the original pipeline.
    
    Args:
        products_path: Path to Products.txt
        
    Returns:
        DataFrame with Orange Book products formatted for the pipeline
    """
    print(f"Loading Orange Book products from {products_path}...")
    
    # Load the tab-separated file
    df = pd.read_csv(products_path, sep='\t', dtype={'ApplNo': str, 'ProductNo': str})
    
    print(f"  Loaded {len(df)} product records")
    print(f"  Unique applications: {df['ApplNo'].nunique()}")
    
    # Clean up application numbers - remove leading zeros and convert to int for classification
    df['ApplNo_Int'] = df['ApplNo'].astype(int)
    
    # Determine if NDA or ANDA based on application number
    # ANDAs typically >= 60000 (most are 70000-90000 range or >= 200000)
    # NDAs are typically < 60000
    def classify_appl_type(appl_no):
        if appl_no >= 60000:
            return 'A'  # ANDA
        else:
            return 'N'  # NDA
    
    df['Appl_Type'] = df['ApplNo_Int'].apply(classify_appl_type)
    
    print(f"  Classified as NDAs: {(df['Appl_Type'] == 'N').sum()}")
    print(f"  Classified as ANDAs: {(df['Appl_Type'] == 'A').sum()}")
    
    # Format to match expected Orange Book structure
    # The clean_orange_book function expects these columns:
    # Ingredient, DF;Route, Trade_Name, Applicant, Strength, Appl_Type, Appl_No, Product_No, 
    # TE_Code, Approval_Date, RLD, RS, Type
    
    orange_book_formatted = pd.DataFrame({
        'Ingredient': df['ActiveIngredient'],
        'DF;Route': df['Form'],  # Form contains both dosage form and route
        'Trade_Name': df['DrugName'],
        'Applicant': '',  # Not in Products.txt, will be filled from Applications.txt later
        'Strength': df['Strength'],
        'Appl_Type': df['Appl_Type'],
        'Appl_No': df['ApplNo'],  # Keep as string
        'Product_No': df['ProductNo'],  # Keep as string
        'TE_Code': '',  # Not available in Products.txt
        'Approval_Date': '',  # Not available in Products.txt
        'RLD': df['ReferenceDrug'].astype(str),
        'RS': df['ReferenceStandard'].astype(str),
        'Type': 'RX'  # Default, not available in Products.txt
    })
    
    return orange_book_formatted


def preprocess_products_data(products_df: pd.DataFrame, main_table_df: pd.DataFrame) -> tuple:
    """Preprocess products data to match the format expected by the matching algorithm.
    
    Args:
        products_df: Raw products DataFrame from Products.txt (already formatted for Orange Book)
        main_table_df: Main table with NDA and Company info
        
    Returns:
        Tuple of (main_table_clean, orange_book_clean)
    """
    print("Preprocessing products data...")
    
    # Import the clean_orange_book function from preprocess module
    from preprocess import clean_orange_book, str_squish
    
    # Main table is already in the right format from get_companyNDAs
    main_table_clean = main_table_df.copy()
    
    # Ensure Appl_No is string type with 6-digit zero-padding to match Orange Book format
    # Orange Book uses format like '000004', '020234', etc.
    main_table_clean['Appl_No'] = main_table_clean['NDA_Appl_No'].apply(lambda x: str(int(x)).zfill(6))
    
    # Clean the orange book data using the standard cleaning function
    orange_book_clean = clean_orange_book(products_df)
    
    # Ensure Appl_No is string type for consistency (it should already be from clean_orange_book)
    orange_book_clean['Appl_No'] = orange_book_clean['Appl_No'].astype(str)
    
    # Filter to only include applications we care about
    # Keep all NDAs from main_table and all potential ANDAs
    nda_list_formatted = main_table_clean['Appl_No'].unique()
    
    # Keep NDAs that are in our main table
    nda_products = orange_book_clean[
        (orange_book_clean['Appl_Type'] == 'N') & 
        (orange_book_clean['Appl_No'].isin(nda_list_formatted))
    ]
    
    # Keep all ANDAs
    anda_products = orange_book_clean[orange_book_clean['Appl_Type'] == 'A']
    
    # Combine
    orange_book_clean = pd.concat([nda_products, anda_products], ignore_index=True)
    
    print(f"  Main table: {len(main_table_clean)} NDAs")
    print(f"  Orange Book: {len(orange_book_clean)} products")
    print(f"    - NDA products: {(orange_book_clean['Appl_Type'] == 'N').sum()}")
    print(f"    - ANDA products: {(orange_book_clean['Appl_Type'] == 'A').sum()}")
    
    return main_table_clean, orange_book_clean


def main():
    """Main analysis pipeline for 2025 Orange Book data."""
    print("Starting NDA-ANDA monopoly time analysis (2025 Orange Book)...")
    print("=" * 70)
    
    # Step 1: Load NDAs from collected data
    print("\n1. Loading NDAs from collected data...")
    nda_list = get_nda_list(COLLECTED_DATA_PATH)
    nda_dates = get_nda_approval_dates(COLLECTED_DATA_PATH)
    print(f"   Testing {len(nda_list)} NDAs")
    print()
    
    # Step 2: Load company info from Applications.txt
    print("2. Loading company information from Applications.txt...")
    main_table_clean = create_main_table_equivalent(
        APPLICATIONS_PATH, 
        nda_list=nda_list,
        nda_dates=nda_dates
    )
    print(f"   Loaded {len(main_table_clean)} NDA-company mappings")
    print()
    
    # Step 3: Load Orange Book products from Products.txt
    print("3. Loading Orange Book products from Products.txt...")
    products_df = load_products_txt(PRODUCTS_PATH)
    print()
    
    # Step 4: Preprocess data
    print("4. Preprocessing data...")
    main_table_clean, orange_book_clean = preprocess_products_data(products_df, main_table_clean)
    print()
    
    # Step 5: Run NDA-ANDA matching
    print("5. Running NDA-ANDA matching algorithm...")
    match_data = match_ndas_to_andas(main_table_clean, orange_book_clean)
    print(f"   Generated NDA summary with {len(match_data.nda_summary)} NDAs")
    print(f"   Found {len(match_data.anda_matches)} total NDA-ANDA matches")
    print()
    
    # Step 6: Run company validation on matches
    print("6. Running company validation using FDA approval letters...")
    print("   This will query the FDA API and extract company references from PDFs")
    print("   (This may take several minutes depending on the number of ANDAs)")
    print()
    
    validated_matches, rejected_matches, validation_details = nda_anda_company_validation(
        match_data=match_data,
        orange_book_clean=orange_book_clean,
        main_table_clean=main_table_clean,
        max_andas_to_process=None  # Process ALL ANDAs (full production run)
    )
    
    print(f"   Validation complete:")
    print(f"   - Validated matches: {len(validated_matches)}")
    print(f"   - Rejected matches: {len(rejected_matches)}")
    print(f"   - PDFs processed: {len(validation_details['anda_pdf_urls'])}")
    print()
    
    # Step 7: Create validated match data object
    print("7. Creating validated match dataset...")
    validated_match_data = create_validated_match_data(match_data, validated_matches)
    print(f"   Updated match data with {len(validated_match_data.anda_matches)} validated matches")
    print()
    
    # Step 8: Generate diagnostic outputs with validated data
    print("8. Generating diagnostic summaries and calculating monopoly times...")
    print("   (Using validated matches only - ANDAs filtered by date and company)")
    outputs = build_postprocess_outputs(validated_match_data)
    display_postprocess_summary(outputs)
    
    # Step 9: Create monopoly time visualization
    print("9. Creating monopoly time scatter plot...")
    plot_monopoly_scatter(outputs["nda_monopoly_times"], show=True)
    print("   Monopoly time scatter plot generated successfully!")
    print()
    
    # Step 10: Export results to text files
    print("10. Exporting results to text files...")
    export_nda_anda_matches(validated_match_data, filename="final_nda_anda_matches_2025.txt")
    export_pdf_extraction_status(validation_details, filename="pdf_extraction_status_2025.txt")
    export_monopoly_times(outputs["nda_monopoly_times"], filename="monopoly_times_from_matches_2025.csv")
    print("   [SAVED] final_nda_anda_matches_2025.txt")
    print("   [SAVED] pdf_extraction_status_2025.txt")
    print("   [SAVED] monopoly_times_from_matches_2025.csv")
    print()
    
    print("Analysis complete!")
    print("=" * 70)
    
    # Return results for further analysis
    return {
        'original_match_data': match_data,
        'validated_match_data': validated_match_data,
        'validated_matches': validated_matches,
        'rejected_matches': rejected_matches,
        'validation_details': validation_details,
        'outputs': outputs,
        'nda_list': nda_list,
        'nda_dates': nda_dates
    }


def export_nda_anda_matches(validated_match_data, filename="final_nda_anda_matches_2025.txt"):
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
        f.write("FINAL NDA-ANDA MATCHES (2025 Orange Book - After Company Validation)\n")
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
        
        # Count unique ANDAs
        unique_andas = set()
        for andas in nda_anda_map.values():
            unique_andas.update(andas)
        
        f.write(f"Total unique validated ANDA matches: {len(unique_andas)}\n")
        f.write("\n" + "-" * 80 + "\n\n")
        
        for nda_num in sorted(nda_anda_map.keys()):
            anda_list = sorted(set(nda_anda_map[nda_num]))  # Remove duplicates and sort
            f.write(f"NDA{nda_num}: {', '.join(anda_list)}\n")


def export_pdf_extraction_status(validation_details, filename="pdf_extraction_status_2025.txt"):
    """Export PDF extraction status to a text file.
    
    Shows which approval letters were successfully extracted and which failed.
    
    Args:
        validation_details: Dictionary with validation details including PDF URLs and company references
        filename: Output filename
    """
    with open(filename, 'w', encoding='utf-8') as f:
        # Write header
        f.write("=" * 80 + "\n")
        f.write("FDA APPROVAL LETTER PDF EXTRACTION STATUS (2025 Orange Book)\n")
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
        if total_andas > 0:
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
                    f.write(f"    Status: [FAILED] PDF not found via FDA API\n")
                elif not company_ref:
                    f.write(f"    Status: [FAILED] PDF found but company reference extraction failed\n")
                    f.write(f"    PDF URL: {pdf_url}\n")
                
                f.write("\n")


def export_monopoly_times(monopoly_times_df, filename="monopoly_times_from_matches_2025.csv"):
    """Export monopoly times to CSV file.
    
    Args:
        monopoly_times_df: DataFrame with monopoly time calculations
        filename: Output filename
    """
    monopoly_times_df.to_csv(filename, index=False)


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
    # results['nda_list'] - list of NDAs from collected data
    # results['nda_dates'] - NDA approval dates
