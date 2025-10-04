"""Test script to check accessibility of approval letter PDFs specifically for ANDA applications."""

import requests
import json
import pandas as pd
from typing import Dict, List, Any, Optional
import time
from urllib.parse import urlparse

# FDA API Configuration
API_KEY = "VVwIr3zalK3V1THgAW2mym0DndMnlMBw1oBegvg6"
BASE_URL = "https://api.fda.gov/drug"

class ANDAApprovalLetterTester:
    """Test class for checking ANDA approval letter PDF accessibility."""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = BASE_URL
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
    def make_api_request(self, endpoint: str, search_params: str = "", limit: int = 100) -> Optional[Dict]:
        """Make a request to the FDA API."""
        url = f"{self.base_url}/{endpoint}.json"
        params = {
            "api_key": self.api_key,
            "limit": limit
        }
        
        if search_params:
            params["search"] = search_params
            
        try:
            print(f"Making API request for {limit} records...")
            if search_params:
                print(f"Search filter: {search_params}")
            response = self.session.get(url, params=params)
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            print(f"API Request failed: {e}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"Response status: {e.response.status_code}")
                print(f"Response text: {e.response.text}")
            return None
    
    def check_url_accessibility(self, url: str) -> Dict[str, Any]:
        """Check if a URL is accessible without downloading the full content."""
        try:
            # Use HEAD request to check accessibility without downloading
            response = self.session.head(url, timeout=10, allow_redirects=True)
            
            return {
                'url': url,
                'accessible': response.status_code == 200,
                'status_code': response.status_code,
                'content_type': response.headers.get('content-type', ''),
                'content_length': response.headers.get('content-length', ''),
                'error': None
            }
            
        except requests.exceptions.RequestException as e:
            return {
                'url': url,
                'accessible': False,
                'status_code': None,
                'content_type': '',
                'content_length': '',
                'error': str(e)
            }
    
    def extract_approval_letters_from_applications(self, applications: List[Dict]) -> List[Dict]:
        """Extract approval letter information from ANDA applications."""
        approval_letters = []
        
        for app in applications:
            app_number = app.get('application_number', 'Unknown')
            sponsor = app.get('sponsor_name', 'Unknown')
            submissions = app.get('submissions', [])
            
            # Verify this is an ANDA application
            if not app_number.startswith('ANDA'):
                continue
                
            for submission in submissions:
                submission_type = submission.get('submission_type', '')
                submission_status = submission.get('submission_status', '')
                submission_number = submission.get('submission_number', '')
                
                # Check if this submission has application documents
                app_docs = submission.get('application_docs', [])
                
                for doc in app_docs:
                    doc_type = doc.get('type', '').lower()
                    doc_url = doc.get('url', '')
                    doc_date = doc.get('date', '')
                    doc_id = doc.get('id', '')
                    
                    # Look for documents that are likely approval letters
                    if doc_type == 'letter' or 'letter' in doc_url.lower() or 'ltr' in doc_url.lower():
                        approval_letters.append({
                            'application_number': app_number,
                            'sponsor_name': sponsor,
                            'submission_type': submission_type,
                            'submission_status': submission_status,
                            'submission_number': submission_number,
                            'document_id': doc_id,
                            'document_type': doc_type,
                            'document_date': doc_date,
                            'document_url': doc_url,
                            'is_orig_submission': submission_type == 'ORIG',
                            'is_approved': submission_status == 'AP'
                        })
        
        return approval_letters
    
    def get_anda_applications(self, limit: int = 500) -> List[Dict]:
        """Get ANDA applications specifically."""
        print(f"Fetching ANDA applications (limit: {limit})...")
        
        # Search specifically for ANDA applications
        search_query = 'application_number:ANDA*'
        api_result = self.make_api_request("drugsfda", search_query, limit=limit)
        
        if not api_result or 'results' not in api_result:
            print("Failed to fetch ANDA data from FDA API")
            return []
        
        applications = api_result['results']
        
        # Filter to ensure we only have ANDA applications
        anda_applications = [app for app in applications if app.get('application_number', '').startswith('ANDA')]
        
        print(f"Retrieved {len(anda_applications)} ANDA applications")
        return anda_applications
    
    def test_anda_approval_letter_accessibility(self, num_applications: int = 500) -> pd.DataFrame:
        """Test accessibility of approval letters specifically for ANDA applications."""
        print(f"Testing ANDA approval letter accessibility for up to {num_applications} applications...")
        print("=" * 80)
        
        # Step 1: Get ANDA applications from FDA API
        print("Step 1: Fetching ANDA applications from FDA API...")
        anda_applications = self.get_anda_applications(limit=num_applications)
        
        if not anda_applications:
            print("No ANDA applications found")
            return pd.DataFrame()
        
        # Step 2: Extract approval letters
        print("\nStep 2: Extracting approval letter information from ANDA applications...")
        approval_letters = self.extract_approval_letters_from_applications(anda_applications)
        print(f"Found {len(approval_letters)} potential ANDA approval letter documents")
        
        if not approval_letters:
            print("No approval letters found in ANDA applications")
            return pd.DataFrame()
        
        # Step 3: Test URL accessibility
        print(f"\nStep 3: Testing accessibility of {len(approval_letters)} ANDA approval letter URLs...")
        accessibility_results = []
        
        for i, letter in enumerate(approval_letters):
            print(f"Testing {i+1}/{len(approval_letters)}: {letter['application_number']} - {letter['document_url']}")
            
            # Check URL accessibility
            url_result = self.check_url_accessibility(letter['document_url'])
            
            # Combine letter info with accessibility result
            result = {**letter, **url_result}
            accessibility_results.append(result)
            
            # Small delay to be respectful to the server (reduced for larger batch)
            time.sleep(0.3)
        
        # Step 4: Create summary DataFrame
        df = pd.DataFrame(accessibility_results)
        
        # Print summary statistics
        self.print_anda_accessibility_summary(df, len(anda_applications))
        
        return df
    
    def print_anda_accessibility_summary(self, df: pd.DataFrame, total_anda_apps: int):
        """Print a summary of ANDA accessibility results."""
        print("\n" + "=" * 80)
        print("ANDA APPROVAL LETTER ACCESSIBILITY SUMMARY")
        print("=" * 80)
        
        total_letters = len(df)
        accessible_letters = len(df[df['accessible'] == True])
        inaccessible_letters = total_letters - accessible_letters
        
        print(f"Total ANDA applications analyzed: {total_anda_apps}")
        print(f"ANDA applications with approval letters: {df['application_number'].nunique()}")
        print(f"Total ANDA approval letters found: {total_letters}")
        print(f"Accessible ANDA PDFs: {accessible_letters} ({accessible_letters/total_letters*100:.1f}%)")
        print(f"Inaccessible ANDA PDFs: {inaccessible_letters} ({inaccessible_letters/total_letters*100:.1f}%)")
        
        # Breakdown by submission type
        print(f"\nBreakdown by submission type:")
        submission_breakdown = df.groupby('submission_type').agg({
            'accessible': ['count', 'sum']
        }).round(2)
        submission_breakdown.columns = ['Total', 'Accessible']
        submission_breakdown['Accessibility_Rate'] = (submission_breakdown['Accessible'] / submission_breakdown['Total'] * 100).round(1)
        print(submission_breakdown)
        
        # Breakdown by approval status
        print(f"\nBreakdown by approval status:")
        status_breakdown = df.groupby('submission_status').agg({
            'accessible': ['count', 'sum']
        }).round(2)
        status_breakdown.columns = ['Total', 'Accessible']
        status_breakdown['Accessibility_Rate'] = (status_breakdown['Accessible'] / status_breakdown['Total'] * 100).round(1)
        print(status_breakdown)
        
        # Sample application numbers
        sample_apps = df['application_number'].unique()[:10]
        print(f"\nSample ANDA application numbers found:")
        for i, app_num in enumerate(sample_apps, 1):
            print(f"  {i}. {app_num}")
        
        # Status codes
        print(f"\nHTTP Status codes encountered:")
        status_codes = df['status_code'].value_counts().sort_index()
        for code, count in status_codes.items():
            print(f"  {code}: {count} ({count/total_letters*100:.1f}%)")
        
        # Sample accessible URLs
        accessible_urls = df[df['accessible'] == True]['document_url'].head(5).tolist()
        if accessible_urls:
            print(f"\nSample accessible ANDA approval letter URLs:")
            for i, url in enumerate(accessible_urls, 1):
                print(f"  {i}. {url}")
        
        # Sample inaccessible URLs with errors
        inaccessible_df = df[df['accessible'] == False]
        if not inaccessible_df.empty:
            print(f"\nSample inaccessible ANDA URLs and reasons:")
            for i, (_, row) in enumerate(inaccessible_df.head(3).iterrows(), 1):
                print(f"  {i}. {row['document_url']}")
                print(f"     Status: {row['status_code']}, Error: {row['error']}")
    
    def save_results_to_csv(self, df: pd.DataFrame, filename: str = "anda_approval_letter_accessibility_test.csv"):
        """Save ANDA results to CSV file."""
        if not df.empty:
            df.to_csv(filename, index=False)
            print(f"\nANDA results saved to: {filename}")
        else:
            print("\nNo ANDA results to save")

def main():
    """Main function to run the ANDA approval letter accessibility test."""
    print("FDA ANDA Approval Letter Accessibility Test")
    print("=" * 50)
    
    tester = ANDAApprovalLetterTester(API_KEY)
    
    # Test with up to 500 ANDA applications
    results_df = tester.test_anda_approval_letter_accessibility(num_applications=500)
    
    # Save results
    tester.save_results_to_csv(results_df)
    
    print("\n" + "=" * 80)
    print("ANDA TEST COMPLETE")
    print("=" * 80)

if __name__ == "__main__":
    main()