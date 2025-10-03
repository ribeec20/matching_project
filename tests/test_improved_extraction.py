"""
Test the improved reference company extraction with sample URLs.
"""

from extract_from_pdf import FDAApprovalLetterExtractor

def test_improved_extraction():
    """Test the improved reference company extraction."""
    
    # Sample URLs from previous tests
    test_urls = [
        "http://www.accessdata.fda.gov/drugsatfda_docs/appletter/2007/077780s000_ltr.pdf",
        "http://www.accessdata.fda.gov/drugsatfda_docs/appletter/2007/076886s000ltr.pdf", 
        "http://www.accessdata.fda.gov/drugsatfda_docs/appletter/2018/206074Orig1s000ltr.pdf"
    ]
    
    print("Testing improved reference company extraction...")
    print("=" * 60)
    
    extractor = FDAApprovalLetterExtractor()
    
    for i, url in enumerate(test_urls, 1):
        print(f"\n[{i}] Testing: {url.split('/')[-1]}")
        
        # Extract text from PDF
        pdf_text = extractor.parse_pdf_from_url(url)
        
        if pdf_text:
            # Test the improved extraction
            reference_company = extractor.extract_reference_company(pdf_text)
            reference_drug = extractor.extract_reference_drug(pdf_text)
            
            print(f"    Reference Drug: {reference_drug}")
            print(f"    Reference Company: {reference_company}")
            
            # Show relevant text snippet containing "Accordingly the ANDA is approved"
            import re
            approval_sentences = re.findall(r'Accordingly[^.]*?\.', pdf_text, re.IGNORECASE | re.DOTALL)
            if approval_sentences:
                sentence = approval_sentences[0][:300] + "..." if len(approval_sentences[0]) > 300 else approval_sentences[0]
                print(f"    Approval Sentence: {sentence}")
                
                # Also show bioequivalence sentences
                bioequiv_sentences = re.findall(r'Office\s+of\s+Bioequivalence[^.]*?\.', pdf_text, re.IGNORECASE | re.DOTALL)
                if bioequiv_sentences:
                    bio_sentence = bioequiv_sentences[0][:300] + "..." if len(bioequiv_sentences[0]) > 300 else bioequiv_sentences[0]
                    print(f"    Bioequiv Sentence: {bio_sentence}")
            else:
                print("    No 'Accordingly the ANDA is approved' sentence found")
        else:
            print("    Failed to extract PDF text")
        
        print("-" * 60)

if __name__ == "__main__":
    test_improved_extraction()