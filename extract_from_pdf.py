"""
Extract company reference information from FDA approval letter PDFs.
Simplified to focus only on company extraction for NDA-ANDA validation.
"""

import requests
import PyPDF2
from io import BytesIO
import re
import time
import logging
from typing import Dict, Optional

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class PDFCompanyExtractor:
    """Extract company reference text from a single FDA approval letter PDF."""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
    
    def parse_pdf_from_url(self, pdf_url: str) -> str:
        """Parse PDF directly from URL and extract text.
        
        Args:
            pdf_url: URL to the PDF file
            
        Returns:
            Extracted text content or empty string if failed
        """
        try:
            logger.debug(f"Fetching PDF from: {pdf_url}")
            response = self.session.get(pdf_url, timeout=30)
            response.raise_for_status()
            
            pdf_stream = BytesIO(response.content)
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
    
    def extract_reference_company(self, text: str) -> Optional[str]:
        """Extract the bioequivalence determination sentence containing company reference.
        
        Args:
            text: PDF text content
            
        Returns:
            Extracted company reference sentence or None if not found
        """
        bioequiv_patterns = [
            # Pattern 1: Full sentence with company suffix
            r'(The\s+Office\s+of\s+Bioequivalence[^.\n]*(?:\.[^.\n]*)*?(?:Inc\.|LLC|Corporation|Corp\.|Limited|Ltd\.)(?:[^.\n]*(?:\n(?!\n))?)*)',
            r'(Office\s+of\s+Bioequivalence\s+has\s+determined[^.\n]*(?:\.[^.\n]*)*?(?:Inc\.|LLC|Corporation|Corp\.|Limited|Ltd\.)(?:[^.\n]*(?:\n(?!\n))?)*)',
            # Pattern 2: Division of Bioequivalence
            r'(The\s+Division\s+of\s+Bioequivalence\s+has\s+determined[^.\n]*(?:\.[^.\n]*)*?(?:Inc\.|LLC|Corporation|Corp\.|Limited|Ltd\.)(?:[^.\n]*(?:\n(?!\n))?)*)',
            r'(Division\s+of\s+Bioequivalence\s+has\s+determined[^.\n]*(?:\.[^.\n]*)*?(?:Inc\.|LLC|Corporation|Corp\.|Limited|Ltd\.)(?:[^.\n]*(?:\n(?!\n))?)*)',
            # Pattern 3: Fallback - capture full bioequivalence statement
            r'(The\s+(?:Office|Division)\s+of\s+Bioequivalence[^\n]*(?:\n(?!\n)[^\n]*)*)',
            r'((?:Office|Division)\s+of\s+Bioequivalence\s+has\s+determined[^\n]*(?:\n(?!\n)[^\n]*)*)',
            # Pattern 4: Bioequivalent and therapeutically equivalent
            r'([^.\n]*bioequivalent\s+and\s+therapeutically\s+equivalent\s+to\s+the\s+reference\s+listed\s+drug[^\n]*(?:\n(?!\n)[^\n]*)*)',
        ]
        
        for pattern in bioequiv_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE | re.DOTALL)
            if matches:
                sentence = matches[0].strip()
                # Normalize whitespace
                sentence = re.sub(r'\s+', ' ', sentence)
                return sentence
        
        return None
    
    def get_company_reference(self, pdf_url: str) -> Optional[str]:
        """Extract company reference from PDF URL.
        
        Args:
            pdf_url: URL to the FDA approval letter PDF
            
        Returns:
            Extracted company reference text or None if not found
        """
        pdf_text = self.parse_pdf_from_url(pdf_url)
        
        if not pdf_text:
            return None
        
        return self.extract_reference_company(pdf_text)


class BatchPDFExtractor:
    """Process multiple ANDA PDFs to extract company references."""
    
    def __init__(self, rate_limit_delay: float = 1.0):
        """Initialize batch extractor.
        
        Args:
            rate_limit_delay: Delay in seconds between PDF requests
        """
        self.extractor = PDFCompanyExtractor()
        self.rate_limit_delay = rate_limit_delay
    
    def extract_companies_from_andas(self, anda_pdf_urls: Dict[str, str]) -> Dict[str, Optional[str]]:
        """Extract company references from multiple ANDA approval letters.
        
        Args:
            anda_pdf_urls: Dictionary mapping ANDA number to PDF URL
            
        Returns:
            Dictionary mapping ANDA number to extracted company reference text
        """
        company_references = {}
        total = len(anda_pdf_urls)
        
        logger.info(f"Extracting company references from {total} ANDA approval letters...")
        
        for i, (anda_num, pdf_url) in enumerate(anda_pdf_urls.items(), 1):
            logger.info(f"Processing ANDA {anda_num} ({i}/{total}): {pdf_url}")
            
            try:
                company_ref_text = self.extractor.get_company_reference(pdf_url)
                company_references[anda_num] = company_ref_text
                
                if company_ref_text:
                    logger.info(f"✓ Successfully extracted reference text for ANDA {anda_num}")
                else:
                    logger.warning(f"✗ No company reference text found for ANDA {anda_num}")
                    
            except Exception as e:
                logger.error(f"Error processing ANDA {anda_num}: {str(e)}")
                company_references[anda_num] = None
            
            # Rate limiting
            if i < total:
                time.sleep(self.rate_limit_delay)
        
        # Summary
        found_count = sum(1 for ref in company_references.values() if ref is not None)
        logger.info(f"Extraction complete: {found_count}/{total} company references found ({found_count/total*100:.1f}%)")
        
        return company_references