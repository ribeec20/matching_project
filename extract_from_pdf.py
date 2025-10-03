"""
Extract structured data from FDA approval letter PDFs.
This script processes a list of PDF URLs and extracts key information into a CSV file.
"""

import requests
import PyPDF2
from io import BytesIO
import pandas as pd
import re
import os
from typing import List, Dict, Optional
import time
from datetime import datetime
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class FDAApprovalLetterExtractor:
    """Extract structured data from FDA approval letter PDFs."""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
    
    def parse_pdf_from_url(self, pdf_url: str) -> str:
        """Parse PDF directly from URL without downloading to disk."""
        try:
            logger.info(f"Fetching PDF from: {pdf_url}")
            response = self.session.get(pdf_url, timeout=30)
            response.raise_for_status()
            
            pdf_stream = BytesIO(response.content)
            
            # Parse with PyPDF2
            pdf_reader = PyPDF2.PdfReader(pdf_stream)
            text = ""
            for page in pdf_reader.pages:
                page_text = page.extract_text()
                if page_text:
                    text += page_text + "\n"
            
            return text
            
        except Exception as e:
            logger.error(f"Error parsing PDF {pdf_url}: {str(e)}")
            return ""
    
    def extract_approval_date(self, text: str) -> Optional[str]:
        """Extract approval date from PDF text."""
        # Look for date patterns near "approved" or "effective"
        date_patterns = [
            r'approved[,\s]+effective[,\s]+on\s+([A-Za-z]+ \d{1,2}, \d{4})',
            r'approved[,\s]+effective[,\s]+(\d{1,2}/\d{1,2}/\d{4})',
            r'approved[,\s]+effective[,\s]+(\d{4}-\d{2}-\d{2})',
            r'date of this letter[:\s]*([A-Za-z]+ \d{1,2}, \d{4})',
            r'(\d{1,2}/\d{1,2}/\d{4})',  # Generic date format
        ]
        
        text_lower = text.lower()
        for pattern in date_patterns:
            matches = re.findall(pattern, text_lower, re.IGNORECASE)
            if matches:
                return matches[0].strip()
        
        return None
    
    def extract_drug_name(self, text: str) -> Optional[str]:
        """Extract the drug name being approved."""
        # Look for patterns like "your [Drug Name] [dosage form]"
        drug_patterns = [
            r'your\s+([A-Za-z]+(?:\s+[A-Za-z]+)*)\s+(?:Tablets?|Capsules?|Injection|Solution|Suspension|Cream|Ointment|Gel)',
            r'determined\s+your\s+([A-Za-z]+(?:\s+[A-Za-z]+)*)\s+(?:Tablets?|Capsules?|Injection)',
            r'bioequivalent[^.]*your\s+([A-Za-z]+(?:\s+[A-Za-z]+)*)\s+(?:Tablets?|Capsules?|Injection)',
        ]
        
        for pattern in drug_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            if matches:
                # Clean up the drug name
                drug_name = matches[0].strip()
                # Remove common words that might be captured
                exclusions = ['and', 'or', 'the', 'to', 'be', 'has', 'determined']
                words = drug_name.split()
                clean_words = [w for w in words if w.lower() not in exclusions]
                if clean_words:
                    return ' '.join(clean_words)
        
        return None
    
    def extract_dosage_form(self, text: str) -> Optional[str]:
        """Extract the dosage form (tablets, capsules, etc.)."""
        dosage_patterns = [
            r'your\s+[A-Za-z\s]+\s+(Tablets?|Capsules?|Injection|Solution|Suspension|Cream|Ointment|Gel|Delayed-Release\s+Capsules?)',
            r'determined\s+your\s+[A-Za-z\s]+\s+(Tablets?|Capsules?|Injection)',
        ]
        
        for pattern in dosage_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            if matches:
                return matches[0].strip()
        
        return None
    
    def extract_strengths(self, text: str) -> Optional[str]:
        """Extract dosage strengths."""
        # Look for patterns like "2.5 mg, 5 mg, 10 mg" or "20 mg/10 mL"
        strength_patterns = [
            r'(\d+(?:\.\d+)?\s*mg(?:/\d+(?:\.\d+)?\s*mL)?(?:,?\s*(?:and\s+)?\d+(?:\.\d+)?\s*mg(?:/\d+(?:\.\d+)?\s*mL)?)*)',
            r'(\d+(?:\.\d+)?\s*mg(?:/\d+(?:\.\d+)?\s*mL)?)',
        ]
        
        for pattern in strength_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            if matches:
                # Get the longest match (most comprehensive)
                longest_match = max(matches, key=len)
                return longest_match.strip()
        
        return None
    
    def extract_reference_drug(self, text: str) -> Optional[str]:
        """Extract the reference listed drug (RLD) name."""
        rld_patterns = [
            r'reference listed drug[^,]*,\s*([A-Za-z]+(?:\s+[A-Za-z]+)*)\s+(?:Tablets?|Capsules?|Injection)',
            r'RLD[^,]*,\s*([A-Za-z]+(?:\s+[A-Za-z]+)*)\s+(?:Tablets?|Capsules?|Injection)',
            r'therapeutically equivalent to[^,]*,\s*([A-Za-z]+(?:\s+[A-Za-z]+)*)\s+(?:Tablets?|Capsules?|Injection)',
        ]
        
        for pattern in rld_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            if matches:
                return matches[0].strip()
        
        return None
    
    def extract_reference_company(self, text: str) -> Optional[str]:
        """Extract the complete bioequivalence determination sentence."""
        # Look for the complete sentence containing bioequivalence determination
        # Use paragraph breaks (\n\n) or multiple newlines instead of periods to avoid cutting off at decimals
        bioequiv_patterns = [
            # Pattern 1: Full sentence starting with "The Office of Bioequivalence" until paragraph break
            r'(The\s+Office\s+of\s+Bioequivalence[^.\n]*(?:\.[^.\n]*)*?(?:Inc\.|LLC|Corporation|Corp\.|Limited|Ltd\.)(?:[^.\n]*(?:\n(?!\n))?)*)',
            # Pattern 2: Alternative without "The" - capture until company name or paragraph
            r'(Office\s+of\s+Bioequivalence\s+has\s+determined[^.\n]*(?:\.[^.\n]*)*?(?:Inc\.|LLC|Corporation|Corp\.|Limited|Ltd\.)(?:[^.\n]*(?:\n(?!\n))?)*)',
            # Pattern 3: Division of Bioequivalence variant - capture until company or paragraph
            r'(The\s+Division\s+of\s+Bioequivalence\s+has\s+determined[^.\n]*(?:\.[^.\n]*)*?(?:Inc\.|LLC|Corporation|Corp\.|Limited|Ltd\.)(?:[^.\n]*(?:\n(?!\n))?)*)',
            r'(Division\s+of\s+Bioequivalence\s+has\s+determined[^.\n]*(?:\.[^.\n]*)*?(?:Inc\.|LLC|Corporation|Corp\.|Limited|Ltd\.)(?:[^.\n]*(?:\n(?!\n))?)*)',
            # Pattern 4: Fallback - capture from bioequivalence until double newline (paragraph break)
            r'(The\s+(?:Office|Division)\s+of\s+Bioequivalence[^\n]*(?:\n(?!\n)[^\n]*)*)',
            r'((?:Office|Division)\s+of\s+Bioequivalence\s+has\s+determined[^\n]*(?:\n(?!\n)[^\n]*)*)',
            # Pattern 5: More flexible bioequivalence determination patterns - until paragraph break
            r'([^.\n]*bioequivalent\s+and\s+therapeutically\s+equivalent\s+to\s+the\s+reference\s+listed\s+drug[^\n]*(?:\n(?!\n)[^\n]*)*)',
            # Pattern 6: Handle cases with "Accordingly" containing bioequivalence info - until paragraph break
            r'(Accordingly[^.\n]*bioequivalent[^.\n]*therapeutically\s+equivalent[^.\n]*reference\s+listed\s+drug[^\n]*(?:\n(?!\n)[^\n]*)*)',
        ]
        
        for pattern in bioequiv_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE | re.DOTALL)
            if matches:
                sentence = matches[0].strip()
                # Clean up any extra whitespace and normalize spacing
                sentence = re.sub(r'\s+', ' ', sentence)
                # Remove any trailing incomplete words or punctuation
                sentence = re.sub(r'\s+$', '', sentence)
                
                # Ensure sentence starts with a capital letter for readability
                if sentence and not sentence[0].isupper():
                    # Find the start of the actual sentence content
                    words = sentence.split()
                    for i, word in enumerate(words):
                        if word[0].isupper() or word.lower() in ['the', 'office', 'division', 'accordingly']:
                            sentence = ' '.join(words[i:])
                            break
                return sentence
        
        return None
    
    def _clean_company_name(self, company: str) -> str:
        """Clean and standardize company name."""
        # Remove common prefixes and suffixes
        company = re.sub(r'\s*\([^)]*\)\s*$', '', company)  # Remove parenthetical
        company = re.sub(r'^of\s+', '', company, flags=re.IGNORECASE)  # Remove "of" prefix
        company = company.strip()
        
        # Add common suffixes if they seem to be missing and company name looks incomplete
        if company and not any(suffix in company.upper() for suffix in ['INC', 'LLC', 'CORP', 'LTD', 'CORPORATION', 'LIMITED']):
            # Check if it's a known company pattern that should have a suffix
            known_patterns = [
                (r'GlaxoSmithKline', 'GlaxoSmithKline'),
                (r'Pfizer', 'Pfizer Inc.'),
                (r'Novartis', 'Novartis Pharmaceuticals Corporation'),
                (r'Hospira', 'Hospira Inc.'),
                (r'Forest', 'Forest Laboratories LLC'),
                (r'Takeda', 'Takeda Pharmaceuticals U.S.A., Inc.'),
            ]
            
            for pattern, replacement in known_patterns:
                if re.search(pattern, company, re.IGNORECASE):
                    return replacement
        
        return company
    
    def extract_anda_number(self, text: str) -> Optional[str]:
        """Extract ANDA number from the text."""
        anda_patterns = [
            r'ANDA\s*(\d{6,})',
            r'Application\s*(?:No\.?|Number)\s*:?\s*ANDA\s*(\d{6,})',
        ]
        
        for pattern in anda_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            if matches:
                return f"ANDA{matches[0]}"
        
        return None
    
    def extract_applicant_company(self, text: str) -> Optional[str]:
        """Extract the applicant company name."""
        # Look for company names in common locations
        company_patterns = [
            r'(?:Dear|To:)\s+([A-Za-z][^,\n]*?)(?:,|\n)',  # After "Dear" or "To:"
            r'([A-Za-z][^,\n]*?),?\s*(?:Inc\.?|LLC|Corporation|Corp\.?|Limited|Ltd\.?)',  # Company suffixes
        ]
        
        for pattern in company_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            if matches:
                company = matches[0].strip()
                # Filter out obviously wrong matches
                if len(company) > 3 and not any(word in company.lower() for word in ['dear', 'to:', 'sir', 'madam']):
                    return company
        
        return None
    
    def extract_structured_data(self, pdf_url: str) -> Dict:
        """Extract all structured data from a PDF."""
        pdf_text = self.parse_pdf_from_url(pdf_url)
        
        if not pdf_text:
            return {
                'url': pdf_url,
                'status': 'failed',
                'error': 'Could not extract text from PDF'
            }
        
        extracted_data = {
            'url': pdf_url,
            'status': 'success',
            'approval_date': self.extract_approval_date(pdf_text),
            'anda_number': self.extract_anda_number(pdf_text),
            'drug_name': self.extract_drug_name(pdf_text),
            'dosage_form': self.extract_dosage_form(pdf_text),
            'strengths': self.extract_strengths(pdf_text),
            'reference_drug': self.extract_reference_drug(pdf_text),
            'reference_company': self.extract_reference_company(pdf_text),
            'applicant_company': self.extract_applicant_company(pdf_text),
            'extracted_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'text_length': len(pdf_text),
            'error': None
        }
        
        return extracted_data
    
    def process_url_list(self, pdf_urls: List[str], output_csv: str = 'fda_approval_letters_extracted.csv') -> pd.DataFrame:
        """Process a list of PDF URLs and save results to CSV."""
        results = []
        
        logger.info(f"Processing {len(pdf_urls)} PDF URLs...")
        
        for i, url in enumerate(pdf_urls, 1):
            logger.info(f"Processing {i}/{len(pdf_urls)}: {url}")
            
            try:
                extracted_data = self.extract_structured_data(url)
                results.append(extracted_data)
                
                # Add delay to avoid overwhelming the server
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"Error processing URL {url}: {str(e)}")
                results.append({
                    'url': url,
                    'status': 'error',
                    'error': str(e),
                    'extracted_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                })
        
        # Create DataFrame and save to CSV
        df = pd.DataFrame(results)
        df.to_csv(output_csv, index=False)
        logger.info(f"Results saved to {output_csv}")
        
        # Print summary
        successful = len(df[df['status'] == 'success'])
        failed = len(df[df['status'] != 'success'])
        logger.info(f"Summary: {successful} successful, {failed} failed extractions")
        
        return df

def load_urls_from_analysis_file(analysis_file: str) -> List[str]:
    """Extract PDF URLs from ANDA analysis text file."""
    urls = []
    
    try:
        with open(analysis_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Find all PDF URLs in the analysis file
        url_pattern = r'URL:\s*(http[s]?://[^\s\n]+\.pdf)'
        matches = re.findall(url_pattern, content, re.IGNORECASE)
        
        # Remove duplicates while preserving order
        seen = set()
        for url in matches:
            if url not in seen:
                urls.append(url)
                seen.add(url)
        
        logger.info(f"Found {len(urls)} unique PDF URLs in {analysis_file}")
        return urls
        
    except Exception as e:
        logger.error(f"Error reading analysis file {analysis_file}: {str(e)}")
        return []

def process_from_analysis_file(analysis_file: str, output_csv: str = None) -> pd.DataFrame:
    """Process URLs from an ANDA analysis file."""
    if output_csv is None:
        output_csv = analysis_file.replace('.txt', '_pdf_extraction.csv')
    
    # Load URLs from analysis file
    urls = load_urls_from_analysis_file(analysis_file)
    
    if not urls:
        logger.warning("No URLs found in analysis file")
        return pd.DataFrame()
    
    # Process URLs
    extractor = FDAApprovalLetterExtractor()
    return extractor.process_url_list(urls, output_csv)

def main():
    """Example usage of the FDA approval letter extractor."""
    # Option 1: Process from your analysis files
    analysis_files = [
        'anda_analysis_after_2000_1000.txt',
        'anda_analysis_after_2000_500.txt',
        'anda_analysis_after_2000.txt'
    ]
    
    for analysis_file in analysis_files:
        if os.path.exists(analysis_file):
            logger.info(f"Processing URLs from {analysis_file}")
            results_df = process_from_analysis_file(analysis_file)
            
            if not results_df.empty:
                print(f"\nResults from {analysis_file}:")
                successful = len(results_df[results_df['status'] == 'success'])
                print(f"Successfully extracted data from {successful} PDFs")
                
                # Show sample results
                if successful > 0:
                    sample_df = results_df[results_df['status'] == 'success'][['drug_name', 'reference_drug', 'reference_company', 'applicant_company']].head(5)
                    print(sample_df.to_string(index=False))
            
            break  # Process the first available file
    
    else:
        # Option 2: Process sample URLs if no analysis files found
        logger.info("No analysis files found, using sample URLs")
        sample_urls = [
            "http://www.accessdata.fda.gov/drugsatfda_docs/appletter/2007/077780s000_ltr.pdf",
            "http://www.accessdata.fda.gov/drugsatfda_docs/appletter/2007/076886s000ltr.pdf",
            "http://www.accessdata.fda.gov/drugsatfda_docs/appletter/2018/206074Orig1s000ltr.pdf"
        ]
        
        # Initialize extractor
        extractor = FDAApprovalLetterExtractor()
        
        # Process URLs and save to CSV
        results_df = extractor.process_url_list(sample_urls, 'fda_approval_letters_test.csv')
        
        # Display results
        print("\nExtraction Results:")
        print(results_df[['url', 'status', 'drug_name', 'reference_drug', 'reference_company']].to_string())

if __name__ == "__main__":
    main()