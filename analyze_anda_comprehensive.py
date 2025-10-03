"""Comprehensive analysis of ANDA applications - all data and documents."""

import requests
import json
import pandas as pd
from typing import Dict, List, Any, Optional
import time
from datetime import datetime

# FDA API Configuration
API_KEY = "VVwIr3zalK3V1THgAW2mym0DndMnlMBw1oBegvg6"
BASE_URL = "https://api.fda.gov/drug"

class ANDAComprehensiveAnalyzer:
    """Comprehensive analyzer for ANDA applications - all data and documents."""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = BASE_URL
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        self.output_lines = []
        
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
            response = self.session.get(url, params=params)
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            self.log(f"API Request failed: {e}")
            return None
    
    def log(self, message: str):
        """Add message to output log."""
        print(message)
        self.output_lines.append(message)
    
    def analyze_comprehensive_anda_data(self, num_applications: int = 100):
        """Comprehensive analysis of ANDA applications."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        self.log("=" * 100)
        self.log(f"COMPREHENSIVE ANDA APPLICATION ANALYSIS - {timestamp}")
        self.log("=" * 100)
        self.log(f"Analyzing first {num_applications} ANDA applications...")
        self.log("")
        
        # Get ANDA applications
        self.log("Step 1: Fetching ANDA applications from FDA API...")
        search_query = 'application_number:ANDA*'
        api_result = self.make_api_request("drugsfda", search_query, limit=num_applications)
        
        if not api_result or 'results' not in api_result:
            self.log("Failed to fetch ANDA data from FDA API")
            return
        
        applications = api_result['results']
        anda_applications = [app for app in applications if app.get('application_number', '').startswith('ANDA')]
        
        self.log(f"Retrieved {len(anda_applications)} ANDA applications")
        self.log("")
        
        # Overall statistics
        self.analyze_overall_statistics(anda_applications)
        
        # Detailed analysis of each application
        self.analyze_individual_applications(anda_applications)
        
        # Summary of findings
        self.analyze_summary_findings(anda_applications)
    
    def analyze_overall_statistics(self, applications: List[Dict]):
        """Analyze overall statistics across all applications."""
        self.log("=" * 80)
        self.log("OVERALL STATISTICS")
        self.log("=" * 80)
        
        total_apps = len(applications)
        apps_with_submissions = sum(1 for app in applications if app.get('submissions'))
        apps_with_products = sum(1 for app in applications if app.get('products'))
        
        # Count submissions and documents
        total_submissions = 0
        total_documents = 0
        apps_with_docs = 0
        
        submission_types = {}
        submission_statuses = {}
        document_types = {}
        
        for app in applications:
            submissions = app.get('submissions', [])
            total_submissions += len(submissions)
            
            app_has_docs = False
            for submission in submissions:
                sub_type = submission.get('submission_type', 'Unknown')
                sub_status = submission.get('submission_status', 'Unknown')
                
                submission_types[sub_type] = submission_types.get(sub_type, 0) + 1
                submission_statuses[sub_status] = submission_statuses.get(sub_status, 0) + 1
                
                docs = submission.get('application_docs', [])
                total_documents += len(docs)
                if docs:
                    app_has_docs = True
                
                for doc in docs:
                    doc_type = doc.get('type', 'Unknown')
                    document_types[doc_type] = document_types.get(doc_type, 0) + 1
            
            if app_has_docs:
                apps_with_docs += 1
        
        self.log(f"Total ANDA applications: {total_apps}")
        self.log(f"Applications with submissions: {apps_with_submissions} ({apps_with_submissions/total_apps*100:.1f}%)")
        self.log(f"Applications with products: {apps_with_products} ({apps_with_products/total_apps*100:.1f}%)")
        self.log(f"Applications with documents: {apps_with_docs} ({apps_with_docs/total_apps*100:.1f}%)")
        self.log(f"Total submissions: {total_submissions}")
        self.log(f"Total documents: {total_documents}")
        self.log("")
        
        self.log("Submission Types:")
        for sub_type, count in sorted(submission_types.items(), key=lambda x: x[1], reverse=True):
            self.log(f"  {sub_type}: {count}")
        self.log("")
        
        self.log("Submission Statuses:")
        for status, count in sorted(submission_statuses.items(), key=lambda x: x[1], reverse=True):
            self.log(f"  {status}: {count}")
        self.log("")
        
        self.log("Document Types:")
        for doc_type, count in sorted(document_types.items(), key=lambda x: x[1], reverse=True):
            self.log(f"  {doc_type}: {count}")
        self.log("")
    
    def analyze_individual_applications(self, applications: List[Dict]):
        """Detailed analysis of individual applications."""
        self.log("=" * 80)
        self.log("DETAILED APPLICATION ANALYSIS")
        self.log("=" * 80)
        
        for i, app in enumerate(applications, 1):
            app_number = app.get('application_number', 'Unknown')
            sponsor = app.get('sponsor_name', 'Unknown')
            
            self.log(f"[{i:3d}] APPLICATION: {app_number}")
            self.log(f"      Sponsor: {sponsor}")
            
            # Products analysis
            products = app.get('products', [])
            self.log(f"      Products: {len(products)}")
            if products:
                for j, product in enumerate(products[:2]):  # Show first 2 products
                    self.log(f"        Product {j+1}:")
                    self.log(f"          Dosage Form: {product.get('dosage_form', 'N/A')}")
                    self.log(f"          Marketing Status: {product.get('marketing_status', 'N/A')}")
                    self.log(f"          TE Code: {product.get('te_code', 'N/A')}")
                if len(products) > 2:
                    self.log(f"        ... and {len(products) - 2} more products")
            
            # Submissions analysis
            submissions = app.get('submissions', [])
            self.log(f"      Submissions: {len(submissions)}")
            
            if submissions:
                # Group by type for cleaner display
                submission_summary = {}
                total_docs = 0
                
                for submission in submissions:
                    sub_type = submission.get('submission_type', 'Unknown')
                    sub_status = submission.get('submission_status', 'Unknown')
                    docs = submission.get('application_docs', [])
                    total_docs += len(docs)
                    
                    key = f"{sub_type} ({sub_status})"
                    if key not in submission_summary:
                        submission_summary[key] = {'count': 0, 'docs': 0}
                    submission_summary[key]['count'] += 1
                    submission_summary[key]['docs'] += len(docs)
                
                for sub_info, data in submission_summary.items():
                    self.log(f"        {sub_info}: {data['count']} submissions, {data['docs']} documents")
                
                # Show all available documents/URLs
                if total_docs > 0:
                    self.log(f"      ALL AVAILABLE DOCUMENTS ({total_docs} total):")
                    doc_count = 0
                    for submission in submissions:
                        docs = submission.get('application_docs', [])
                        for doc in docs:
                            doc_count += 1
                            doc_id = doc.get('id', 'N/A')
                            doc_type = doc.get('type', 'N/A')
                            doc_date = doc.get('date', 'N/A')
                            doc_url = doc.get('url', 'N/A')
                            
                            self.log(f"        [{doc_count:2d}] Type: {doc_type} | Date: {doc_date} | ID: {doc_id}")
                            self.log(f"             URL: {doc_url}")
                else:
                    self.log("      No documents available")
            else:
                self.log("      No submissions found")
            
            self.log("")  # Blank line between applications
    
    def analyze_summary_findings(self, applications: List[Dict]):
        """Analyze and summarize key findings."""
        self.log("=" * 80)
        self.log("SUMMARY FINDINGS")
        self.log("=" * 80)
        
        # Why only some ANDAs have approval letters
        apps_with_docs = 0
        apps_with_letter_docs = 0
        apps_with_orig_submissions = 0
        apps_with_approved_submissions = 0
        
        letter_reasons = {
            'has_orig_approved': 0,
            'has_orig_no_docs': 0,
            'has_docs_no_letters': 0,
            'no_orig_submissions': 0,
            'no_submissions': 0
        }
        
        for app in applications:
            app_number = app.get('application_number', 'Unknown')
            submissions = app.get('submissions', [])
            
            if not submissions:
                letter_reasons['no_submissions'] += 1
                continue
            
            has_orig = False
            has_orig_approved = False
            has_docs = False
            has_letter_docs = False
            
            for submission in submissions:
                sub_type = submission.get('submission_type', '')
                sub_status = submission.get('submission_status', '')
                docs = submission.get('application_docs', [])
                
                if sub_type == 'ORIG':
                    has_orig = True
                    if sub_status == 'AP':
                        has_orig_approved = True
                
                if docs:
                    has_docs = True
                    apps_with_docs += 1
                    
                    for doc in docs:
                        doc_type = doc.get('type', '').lower()
                        if 'letter' in doc_type or 'ltr' in doc.get('url', '').lower():
                            has_letter_docs = True
                            break
            
            if has_orig:
                apps_with_orig_submissions += 1
            if has_orig_approved:
                apps_with_approved_submissions += 1
            if has_letter_docs:
                apps_with_letter_docs += 1
            
            # Categorize why this app might not have approval letters
            if has_orig_approved and has_letter_docs:
                letter_reasons['has_orig_approved'] += 1
            elif has_orig and not has_docs:
                letter_reasons['has_orig_no_docs'] += 1
            elif has_docs and not has_letter_docs:
                letter_reasons['has_docs_no_letters'] += 1
            elif not has_orig:
                letter_reasons['no_orig_submissions'] += 1
        
        total_apps = len(applications)
        
        self.log("APPROVAL LETTER AVAILABILITY ANALYSIS:")
        self.log(f"Applications with any documents: {apps_with_docs} ({apps_with_docs/total_apps*100:.1f}%)")
        self.log(f"Applications with ORIG submissions: {apps_with_orig_submissions} ({apps_with_orig_submissions/total_apps*100:.1f}%)")
        self.log(f"Applications with approved ORIG submissions: {apps_with_approved_submissions} ({apps_with_approved_submissions/total_apps*100:.1f}%)")
        self.log(f"Applications with approval letters: {apps_with_letter_docs} ({apps_with_letter_docs/total_apps*100:.1f}%)")
        self.log("")
        
        self.log("REASONS FOR MISSING APPROVAL LETTERS:")
        self.log(f"Has ORIG+Approved with letters: {letter_reasons['has_orig_approved']} ({letter_reasons['has_orig_approved']/total_apps*100:.1f}%)")
        self.log(f"Has ORIG but no documents: {letter_reasons['has_orig_no_docs']} ({letter_reasons['has_orig_no_docs']/total_apps*100:.1f}%)")
        self.log(f"Has documents but no letters: {letter_reasons['has_docs_no_letters']} ({letter_reasons['has_docs_no_letters']/total_apps*100:.1f}%)")
        self.log(f"No ORIG submissions: {letter_reasons['no_orig_submissions']} ({letter_reasons['no_orig_submissions']/total_apps*100:.1f}%)")
        self.log(f"No submissions at all: {letter_reasons['no_submissions']} ({letter_reasons['no_submissions']/total_apps*100:.1f}%)")
        self.log("")
        
        self.log("CONCLUSION:")
        self.log("The main reasons why only ~20% of ANDAs have approval letters available:")
        self.log("1. Many ANDAs lack original (ORIG) submissions in the API")
        self.log("2. Some ORIG submissions don't have associated documents")
        self.log("3. Some documents exist but aren't classified as 'letters'")
        self.log("4. Historical ANDAs may have different documentation practices")
        self.log("5. The API may not include all historical documents")
    
    def save_to_file(self, filename: str = "anda_comprehensive_analysis.txt"):
        """Save analysis to text file."""
        with open(filename, 'w', encoding='utf-8') as f:
            for line in self.output_lines:
                f.write(line + '\n')
        
        self.log(f"Analysis saved to: {filename}")

def main():
    """Main function to run comprehensive ANDA analysis."""
    analyzer = ANDAComprehensiveAnalyzer(API_KEY)
    
    # Analyze first 100 ANDA applications comprehensively
    analyzer.analyze_comprehensive_anda_data(num_applications=100)
    
    # Save to file
    analyzer.save_to_file()

if __name__ == "__main__":
    main()