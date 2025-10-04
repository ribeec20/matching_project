"""Test the company validation with known working PDF URLs."""

import pandas as pd
from postprocess import (
    get_nda_companies_from_orange_book, 
    extract_company_references_from_pdfs,
    validate_company_matches
)

def test_with_known_urls():
    """Test validation with known working PDF URLs."""
    print("Testing company validation with known working URLs...")
    
    # Create mock data with known working URLs
    anda_pdf_urls = {
        "077780": "http://www.accessdata.fda.gov/drugsatfda_docs/appletter/2007/077780s000_ltr.pdf",
        "076886": "http://www.accessdata.fda.gov/drugsatfda_docs/appletter/2007/076886s000ltr.pdf", 
        "206074": "http://www.accessdata.fda.gov/drugsatfda_docs/appletter/2018/206074Orig1s000ltr.pdf"
    }
    
    # Test PDF extraction
    print("1. Extracting company references from known PDFs...")
    company_references = extract_company_references_from_pdfs(anda_pdf_urls)
    
    # Display results
    print("\n2. Extracted Company References:")
    for anda_num, ref_text in company_references.items():
        print(f"\nANDA {anda_num}:")
        if ref_text:
            # Show first 300 characters
            display_text = ref_text[:300] + "..." if len(ref_text) > 300 else ref_text
            print(f"   {display_text}")
        else:
            print("   No reference text found")
    
    # Mock NDA companies for testing
    nda_companies = {
        "12345": ["GlaxoSmithKline", "GSK"],
        "67890": ["Parke-Davis", "Pfizer Inc."],
        "11111": ["Hospira Inc.", "Hospira"]
    }
    
    # Create mock match data
    mock_matches = pd.DataFrame({
        'NDA_Appl_No': ['12345', '67890', '11111'],
        'ANDA_Appl_No': ['077780', '076886', '206074'],
        'NDA_Ingredient': ['Carvedilol', 'Fosphenytoin', 'Topotecan']
    })
    
    print("\n3. Testing company validation logic...")
    validated_matches, rejected_matches = validate_company_matches(
        nda_companies, company_references, mock_matches
    )
    
    print(f"\nValidation Results:")
    print(f"   Validated: {len(validated_matches)}")
    print(f"   Rejected: {len(rejected_matches)}")
    
    if len(validated_matches) > 0:
        print("\nValidated Matches:")
        for _, match in validated_matches.iterrows():
            print(f"   NDA {match['NDA_Appl_No']} - ANDA {match['ANDA_Appl_No']}: {match['Matched_Company']}")
    
    if len(rejected_matches) > 0:
        print("\nRejected Matches:")
        for _, match in rejected_matches.iterrows():
            print(f"   NDA {match['NDA_Appl_No']} - ANDA {match['ANDA_Appl_No']}: No company match")
    
    return company_references, validated_matches, rejected_matches

if __name__ == "__main__":
    company_references, validated_matches, rejected_matches = test_with_known_urls()