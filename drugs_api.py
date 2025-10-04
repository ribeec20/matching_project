"""FDA Drugs@FDA API client for retrieving ANDA application data and PDF URLs.

This module provides a clean interface to the FDA openFDA API for querying
ANDA applications and extracting approval letter PDF URLs.
"""

import requests
import time
import logging
from typing import Dict, List, Optional

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# FDA API Configuration
API_KEY = "VVwIr3zalK3V1THgAW2mym0DndMnlMBw1oBegvg6"
BASE_URL = "https://api.fda.gov/drug"


class DrugsAPI:
    """Client for FDA Drugs@FDA API to retrieve ANDA application data."""
    
    def __init__(self, api_key: str = API_KEY, base_url: str = BASE_URL):
        """Initialize the FDA API client.
        
        Args:
            api_key: FDA API key for authentication
            base_url: Base URL for the FDA drug API
        """
        self.api_key = api_key
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
    
    def search_application(self, application_number: str, limit: int = 1) -> Optional[Dict]:
        """Search for a specific drug application by application number.
        
        Args:
            application_number: Full application number (e.g., "ANDA123456")
            limit: Maximum number of results to return
            
        Returns:
            API response JSON or None if request failed
        """
        url = f"{self.base_url}/drugsfda.json"
        params = {
            "api_key": self.api_key,
            "search": f"application_number:{application_number}",
            "limit": limit
        }
        
        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.debug(f"API request failed for {application_number}: {str(e)}")
            return None
    
    def get_anda_data(self, anda_number: str) -> Optional[Dict]:
        """Get data for a specific ANDA application.
        
        Args:
            anda_number: ANDA number (with or without "ANDA" prefix)
            
        Returns:
            Application data dictionary or None if not found
        """
        # Ensure ANDA prefix
        if not anda_number.startswith("ANDA"):
            anda_number = f"ANDA{anda_number}"
        
        response = self.search_application(anda_number)
        
        if response and 'results' in response and response['results']:
            return response['results'][0]
        
        return None
    
    def extract_pdf_urls_from_submission(self, submission: Dict) -> List[str]:
        """Extract PDF URLs from a submission's application documents.
        
        Args:
            submission: Submission dictionary from API response
            
        Returns:
            List of PDF URLs found in submission documents
        """
        pdf_urls = []
        
        app_docs = submission.get('application_docs', [])
        for doc in app_docs:
            doc_url = doc.get('url', '')
            
            # Look specifically for approval letters
            if '/appletter/' in doc_url and doc_url.endswith('.pdf'):
                pdf_urls.append(doc_url)
        
        return pdf_urls
    
    def get_anda_approval_letter_url(self, anda_number: str) -> Optional[str]:
        """Get the approval letter PDF URL for an ANDA application.
        
        Searches through all submissions to find approval letter PDFs.
        
        Args:
            anda_number: ANDA number (with or without "ANDA" prefix)
            
        Returns:
            PDF URL string or None if not found
        """
        app_data = self.get_anda_data(anda_number)
        
        if not app_data:
            logger.debug(f"No API data found for ANDA {anda_number}")
            return None
        
        # Search through all submissions for approval letter PDFs
        submissions = app_data.get('submissions', [])
        
        for submission in submissions:
            pdf_urls = self.extract_pdf_urls_from_submission(submission)
            
            if pdf_urls:
                # Return first approval letter found
                logger.info(f"✓ Found PDF for ANDA {anda_number}: {pdf_urls[0]}")
                return pdf_urls[0]
        
        logger.debug(f"✗ No approval letter PDF found in API for ANDA {anda_number}")
        return None
    
    def get_multiple_anda_pdfs(self, anda_objects: List, rate_limit_delay: float = 0.0) -> Dict[str, Optional[str]]:
        """Get PDF URLs for multiple ANDA applications.
        
        Args:
            anda_objects: List of objects with get_anda_number() method
            rate_limit_delay: Delay in seconds between API requests (default: 0.0 = no delay)
            
        Returns:
            Dictionary mapping ANDA number to PDF URL (or None if not found)
        """
        pdf_urls = {}
        
        logger.info(f"Querying FDA API for {len(anda_objects)} ANDA applications...")
        
        for i, anda in enumerate(anda_objects, 1):
            anda_number = anda.get_anda_number()
            
            try:
                pdf_url = self.get_anda_approval_letter_url(anda_number)
                pdf_urls[anda_number] = pdf_url
                
                # Rate limiting (disabled by default)
                if rate_limit_delay > 0 and i < len(anda_objects):
                    time.sleep(rate_limit_delay)
                    
            except Exception as e:
                logger.error(f"Error processing ANDA {anda_number}: {str(e)}")
                pdf_urls[anda_number] = None
        
        # Summary
        found_count = sum(1 for url in pdf_urls.values() if url is not None)
        success_rate = found_count / len(anda_objects) * 100 if anda_objects else 0
        logger.info(f"API search complete: {found_count}/{len(anda_objects)} PDFs found ({success_rate:.1f}%)")
        
        return pdf_urls
    
    def search_andas_by_wildcard(self, limit: int = 100) -> Optional[Dict]:
        """Search for ANDA applications using wildcard.
        
        Args:
            limit: Maximum number of results (max 1000)
            
        Returns:
            API response with list of ANDA applications
        """
        url = f"{self.base_url}/drugsfda.json"
        params = {
            "api_key": self.api_key,
            "search": "application_number:ANDA*",
            "limit": min(limit, 1000)  # API max is 1000
        }
        
        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"API wildcard search failed: {str(e)}")
            return None