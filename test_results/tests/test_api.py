"""Test script for FDA Drug API - exploring submissions and approval letters."""

import requests
import json
import pandas as pd
from typing import Dict, List, Any, Optional
import time

# FDA API Configuration
API_KEY = "VVwIr3zalK3V1THgAW2mym0DndMnlMBw1oBegvg6"
BASE_URL = "https://api.fda.gov/drug"

class FDAAPITester:
    """Test class for FDA Drug API exploration."""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = BASE_URL
        
    def make_request(self, endpoint: str, search_params: str = "", limit: int = 10) -> Optional[Dict]:
        """Make a request to the FDA API."""
        url = f"{self.base_url}/{endpoint}.json"
        params = {
            "api_key": self.api_key,
            "limit": limit
        }
        
        if search_params:
            params["search"] = search_params
            
        try:
            print(f"Making request to: {url}")
            print(f"Parameters: {params}")
            
            response = requests.get(url, params=params)
            response.raise_for_status()
            
            return response.json()
            
        except requests.exceptions.RequestException as e:
            print(f"API Request failed: {e}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"Response status: {e.response.status_code}")
                print(f"Response text: {e.response.text}")
            return None
            
    def explore_available_endpoints(self):
        """Test different available endpoints."""
        endpoints = [
            "drugsfda",  # Drug approvals data
            "label",     # Drug labeling
            "event",     # Adverse events
            "enforcement", # Enforcement reports
            "ndc"        # National Drug Code directory
        ]
        
        print("="*60)
        print("EXPLORING AVAILABLE ENDPOINTS")
        print("="*60)
        
        for endpoint in endpoints:
            print(f"\n--- Testing endpoint: {endpoint} ---")
            result = self.make_request(endpoint, limit=1)
            if result:
                print(f"✓ {endpoint} endpoint accessible")
                if 'results' in result:
                    print(f"  Records available: {result.get('meta', {}).get('results', {}).get('total', 'Unknown')}")
                    if result['results']:
                        keys = list(result['results'][0].keys())
                        print(f"  Available fields: {keys[:10]}{'...' if len(keys) > 10 else ''}")
            else:
                print(f"✗ {endpoint} endpoint failed")
            time.sleep(1)  # Rate limiting
                
    def explore_drugsfda_submissions(self):
        """Explore the drugsfda endpoint for submission data."""
        print("\n" + "="*60)
        print("EXPLORING DRUGSFDA SUBMISSIONS DATA")
        print("="*60)
        
        # Test basic query
        print("\n1. Basic drugsfda query:")
        result = self.make_request("drugsfda", limit=5)
        
        if result and 'results' in result:
            print(f"Total records: {result.get('meta', {}).get('results', {}).get('total', 'Unknown')}")
            
            # Analyze first record structure
            first_record = result['results'][0]
            print(f"\nFirst record keys: {list(first_record.keys())}")
            
            # Look for submission-related fields
            if 'submissions' in first_record:
                print(f"\nSubmissions structure:")
                submissions = first_record['submissions']
                if submissions:
                    submission_keys = list(submissions[0].keys())
                    print(f"Submission fields: {submission_keys}")
                    
                    # Look for approval letters specifically
                    for i, submission in enumerate(submissions[:3]):
                        print(f"\nSubmission {i+1}:")
                        for key, value in submission.items():
                            if 'url' in key.lower() or 'letter' in key.lower() or 'approval' in key.lower():
                                print(f"  {key}: {value}")
            
            # Look for application numbers to use in specific searches
            app_nums = []
            for record in result['results'][:3]:
                if 'application_number' in record:
                    app_nums.append(record['application_number'])
                    
            print(f"\nSample application numbers: {app_nums}")
            return app_nums
        
        return []
    
    def search_specific_application(self, app_number: str):
        """Search for a specific application number."""
        print(f"\n--- Searching for Application {app_number} ---")
        
        search_query = f'application_number:"{app_number}"'
        result = self.make_request("drugsfda", search_query, limit=1)
        
        if result and 'results' in result and result['results']:
            record = result['results'][0]
            print(f"Found application: {record.get('application_number')}")
            print(f"Sponsor: {record.get('sponsor_name', 'Unknown')}")
            
            # Explore submissions in detail
            if 'submissions' in record:
                submissions = record['submissions']
                print(f"\nFound {len(submissions)} submissions:")
                
                for i, submission in enumerate(submissions):
                    print(f"\n  Submission {i+1}:")
                    for key, value in submission.items():
                        print(f"    {key}: {value}")
                        
                    # Look specifically for approval letters
                    if 'public_notes' in submission:
                        notes = submission['public_notes']
                        if 'approval' in str(notes).lower():
                            print(f"    *** APPROVAL RELATED: {notes}")
                            
            return record
        else:
            print(f"No data found for application {app_number}")
            return None
    
    def search_for_approval_letters(self):
        """Search specifically for records that might contain approval letters."""
        print("\n" + "="*60)
        print("SEARCHING FOR APPROVAL LETTERS")
        print("="*60)
        
        # Search strategies
        search_queries = [
            'submissions.submission_type:"ORIG-1"',  # Original submissions
            'submissions.submission_status:"AP"',     # Approved submissions
            'submissions.public_notes:"approval"',    # Notes containing "approval"
            'sponsor_name:"Pfizer"',                  # Specific sponsor for testing
        ]
        
        for query in search_queries:
            print(f"\n--- Search: {query} ---")
            result = self.make_request("drugsfda", query, limit=3)
            
            if result and 'results' in result:
                print(f"Found {len(result['results'])} records")
                
                for i, record in enumerate(result['results']):
                    print(f"\nRecord {i+1}: {record.get('application_number', 'Unknown')}")
                    print(f"  Sponsor: {record.get('sponsor_name', 'Unknown')}")
                    
                    if 'submissions' in record:
                        for j, submission in enumerate(record['submissions'][:2]):  # First 2 submissions
                            print(f"  Submission {j+1}:")
                            for key, value in submission.items():
                                if any(term in key.lower() for term in ['url', 'letter', 'approval', 'document']):
                                    print(f"    {key}: {value}")
            
            time.sleep(1)  # Rate limiting
    
    def analyze_fields_structure(self):
        """Analyze the structure of available fields."""
        print("\n" + "="*60)
        print("ANALYZING FIELD STRUCTURE")
        print("="*60)
        
        # Load the fields reference
        try:
            fields_df = pd.read_excel('fields.xlsx')
            print("Fields from fields.xlsx:")
            print(fields_df.to_string())
            
            # Look for submission or approval related fields
            submission_fields = fields_df[
                fields_df['Field Name'].str.contains('submission', case=False, na=False) |
                fields_df['Description'].str.contains('submission|approval|letter', case=False, na=False)
            ]
            
            if not submission_fields.empty:
                print("\nSubmission/Approval related fields:")
                print(submission_fields.to_string())
            
        except Exception as e:
            print(f"Could not load fields.xlsx: {e}")
    
    def run_comprehensive_test(self):
        """Run a comprehensive test of the FDA API."""
        print("FDA DRUG API COMPREHENSIVE TEST")
        print("="*60)
        print(f"API Key: {self.api_key[:10]}...")
        print(f"Base URL: {self.base_url}")
        
        # 1. Explore available endpoints
        self.explore_available_endpoints()
        
        # 2. Analyze field structure
        self.analyze_fields_structure()
        
        # 3. Explore drugsfda submissions
        app_numbers = self.explore_drugsfda_submissions()
        
        # 4. Search specific applications
        for app_num in app_numbers[:2]:  # Test first 2 applications
            self.search_specific_application(app_num)
            time.sleep(1)
        
        # 5. Search for approval letters
        self.search_for_approval_letters()
        
        print("\n" + "="*60)
        print("TEST COMPLETE")
        print("="*60)

def main():
    """Main function to run the API tests."""
    print("Starting FDA Drug API Testing...")
    
    tester = FDAAPITester(API_KEY)
    tester.run_comprehensive_test()

if __name__ == "__main__":
    main()