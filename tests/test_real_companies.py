"""Test with accurate company names from the data."""

import pandas as pd
from postprocess import (
    get_nda_companies_from_main_table,
    calculate_text_similarity,
    validate_company_matches
)
from preprocess import preprocess_data

def test_with_real_companies():
    """Test validation with real company names from the data."""
    print("Testing with Real Company Names...")
    print("=" * 35)
    
    # Load data
    main_table_clean, orange_book_clean = preprocess_data(
        "Copy of Main Table - Dosage Strength.xlsx", 
        "OB - Products - Dec 2018.xlsx"
    )
    
    # Get real company names for specific NDAs
    test_ndas = ["19955", "20132", "20297"]
    nda_companies = get_nda_companies_from_main_table(test_ndas, main_table_clean, orange_book_clean)
    
    print("Real company names from Orange Book:")
    for nda, companies in nda_companies.items():
        print(f"   NDA {nda}: {companies}")
    
    # Test with accurate mock reference text using the real company names
    mock_matches = pd.DataFrame({
        'NDA_Appl_No': ['20297', '20132', '19955'],
        'ANDA_Appl_No': ['077780', '088888', '076470'],
        'NDA_Ingredient': ['Carvedilol', 'Sumatriptan', 'Desmopressin'],
        'Validation_Status': ['Unknown', 'Unknown', 'Unknown']
    })
    
    # Use the actual company names from our lookup
    company_references = {
        '077780': "The Division of Bioequivalence has determined your Carvedilol Tablets to be bioequivalent to the reference listed drug, Coreg Tablets of SMITHKLINE BEECHAM",
        '088888': "bioequivalent to reference drug Imitrex of GLAXOSMITHKLINE",  # Should match NDA 20132
        '076470': "bioequivalent to reference drug of FERRING PHARMS INC"  # Should match NDA 19955
    }
    
    print(f"\nMock reference texts:")
    for anda, ref_text in company_references.items():
        print(f"   ANDA {anda}: {ref_text}")
    
    # Test similarity calculations
    print(f"\nSimilarity tests:")
    for nda, companies in nda_companies.items():
        if companies:
            company = companies[0]  # Use first company
            for anda, ref_text in company_references.items():
                if ref_text:
                    similarity = calculate_text_similarity(company, ref_text)
                    print(f"   '{company}' vs ANDA {anda}: {similarity:.2f}")
    
    # Run validation
    print(f"\nRunning validation...")
    validated_matches, rejected_matches = validate_company_matches(
        nda_companies, company_references, mock_matches, similarity_threshold=0.9
    )
    
    print(f"\nValidation Results:")
    print(f"   Validated: {len(validated_matches)}")
    print(f"   Rejected: {len(rejected_matches)}")
    
    if len(validated_matches) > 0:
        print("\nValidated matches:")
        for _, match in validated_matches.iterrows():
            print(f"   ✓ NDA {match['NDA_Appl_No']} - ANDA {match['ANDA_Appl_No']}: {match['Matched_Company']} (similarity: {match['Company_Match_Similarity']:.2f})")
    
    if len(rejected_matches) > 0:
        print("\nRejected matches:")
        for _, match in rejected_matches.iterrows():
            print(f"   ✗ NDA {match['NDA_Appl_No']} - ANDA {match['ANDA_Appl_No']}: {match['Validation_Status']}")
    
    return validated_matches, rejected_matches

if __name__ == "__main__":
    validated_matches, rejected_matches = test_with_real_companies()