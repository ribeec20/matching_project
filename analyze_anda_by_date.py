"""Comprehensive analysis of ANDA applications filtered by date ranges."""

import requests
import json
import pandas as pd
from typing import Dict, List, Any, Optional
import time
from datetime import datetime

# FDA API Configuration
API_KEY = "VVwIr3zalK3V1THgAW2mym0DndMnlMBw1oBegvg6"
BASE_URL = "https://api.fda.gov/drug"

class ANDADateFilteredAnalyzer:
    """Comprehensive analyzer for ANDA applications filtered by date ranges."""
    
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
    
    def parse_date_from_anda(self, app: Dict) -> Optional[int]:
        """Extract year from ANDA application data."""
        # Try to get year from products first
        products = app.get('products', [])
        for product in products:
            # Look for any date-related fields in products
            pass
        
        # Try to get year from submissions
        submissions = app.get('submissions', [])
        for submission in submissions:
            # Look for submission dates
            status_date = submission.get('submission_status_date', '')
            if status_date and len(status_date) >= 4:
                try:
                    year = int(status_date[:4])
                    if 1980 <= year <= 2025:  # Reasonable range
                        return year
                except ValueError:
                    continue
        
        # If no date found, try to infer from application number
        app_number = app.get('application_number', '')
        if app_number.startswith('ANDA'):
            # Extract numeric part
            try:
                numeric_part = app_number.replace('ANDA', '')
                if len(numeric_part) >= 6:
                    # Newer format ANDAs often have higher numbers
                    numeric_value = int(numeric_part)
                    if numeric_value > 200000:  # Post-2010 approximation
                        return 2010
                    elif numeric_value > 100000:  # Post-2005 approximation
                        return 2005
                    elif numeric_value > 75000:  # Post-2000 approximation
                        return 2000
                    elif numeric_value > 50000:  # Post-1990 approximation
                        return 1990
                    else:
                        return 1985  # Older ANDAs
            except ValueError:
                pass
        
        return None
    
    def filter_andas_by_year(self, applications: List[Dict], min_year: int) -> List[Dict]:
        """Filter ANDA applications by minimum year."""
        filtered_apps = []
        
        for app in applications:
            app_year = self.parse_date_from_anda(app)
            if app_year and app_year >= min_year:
                filtered_apps.append(app)
        
        return filtered_apps
    
    def get_anda_applications_large_sample(self, sample_size: int = 500) -> List[Dict]:
        """Get a large sample of ANDA applications to filter from."""
        # Limit to FDA API maximum
        actual_limit = min(sample_size, 1000)
        self.log(f"Fetching large sample of {actual_limit} ANDA applications for filtering...")
        
        search_query = 'application_number:ANDA*'
        api_result = self.make_api_request("drugsfda", search_query, limit=actual_limit)
        
        if not api_result or 'results' not in api_result:
            self.log("Failed to fetch ANDA data from FDA API")
            return []
        
        applications = api_result['results']
        all_applications = [app for app in applications if app.get('application_number', '').startswith('ANDA')]
        
        self.log(f"Retrieved {len(all_applications)} ANDA applications from large sample")
        return all_applications
    
    def analyze_date_filtered_andas(self, min_year: int, target_count: int = 100):
        """Analyze ANDA applications after a specific year."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        self.log("=" * 100)
        self.log(f"ANDA ANALYSIS - APPLICATIONS AFTER {min_year} - {timestamp}")
        self.log("=" * 100)
        self.log(f"Target: First {target_count} ANDA applications after {min_year}")
        self.log("")
        
        # Get large sample and filter by date - increase sample size for larger targets
        sample_size = max(500, target_count * 2)  # At least 2x target or 500, whichever is larger
        self.log(f"Fetching large sample of {sample_size} ANDA applications for filtering...")
        large_sample = self.get_anda_applications_large_sample(sample_size)
        
        if not large_sample:
            self.log("No ANDA applications retrieved")
            return
        
        self.log(f"Retrieved {len(large_sample)} ANDA applications from large sample")
        
        # Add year information to applications
        for app in large_sample:
            app['estimated_year'] = self.parse_date_from_anda(app)
        
        # Filter by year
        filtered_apps = self.filter_andas_by_year(large_sample, min_year)
        
        self.log(f"Found {len(filtered_apps)} applications after {min_year}")
        
        if len(filtered_apps) == 0:
            self.log(f"No applications found after {min_year}")
            return
        
        # Take first target_count applications
        analysis_apps = filtered_apps[:target_count]
        self.log(f"Analyzing first {len(analysis_apps)} applications")
        self.log("")
        
        # Show year distribution
        year_counts = {}
        for app in analysis_apps:
            year = app.get('estimated_year')
            if year:
                year_counts[year] = year_counts.get(year, 0) + 1
        
        self.log("Year distribution of analyzed applications:")
        for year in sorted(year_counts.keys()):
            self.log(f"  {year}: {year_counts[year]} applications")
        self.log("")
        
        # Run the comprehensive analysis
        self.analyze_overall_statistics(analysis_apps, min_year)
        self.analyze_individual_applications(analysis_apps, min_year)
        self.analyze_summary_findings(analysis_apps, min_year)
    
    def analyze_overall_statistics(self, applications: List[Dict], min_year: int):
        """Analyze overall statistics across filtered applications."""
        self.log("=" * 80)
        self.log(f"OVERALL STATISTICS - ANDAs AFTER {min_year}")
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
        
        self.log(f"Total ANDA applications (after {min_year}): {total_apps}")
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
    
    def analyze_individual_applications(self, applications: List[Dict], min_year: int):
        """Detailed analysis of individual applications."""
        self.log("=" * 80)
        self.log(f"DETAILED APPLICATION ANALYSIS - ANDAs AFTER {min_year}")
        self.log("=" * 80)
        
        for i, app in enumerate(applications, 1):
            app_number = app.get('application_number', 'Unknown')
            sponsor = app.get('sponsor_name', 'Unknown')
            estimated_year = app.get('estimated_year', 'Unknown')
            
            self.log(f"[{i:3d}] APPLICATION: {app_number} (Est. Year: {estimated_year})")
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
    
    def analyze_summary_findings(self, applications: List[Dict], min_year: int):
        """Analyze and summarize key findings."""
        self.log("=" * 80)
        self.log(f"SUMMARY FINDINGS - ANDAs AFTER {min_year}")
        self.log("=" * 80)
        
        # Analysis similar to original but with date context
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
        
        self.log(f"APPROVAL LETTER AVAILABILITY ANALYSIS (ANDAs after {min_year}):")
        self.log(f"Applications with any documents: {apps_with_docs} ({apps_with_docs/total_apps*100:.1f}%)")
        self.log(f"Applications with ORIG submissions: {apps_with_orig_submissions} ({apps_with_orig_submissions/total_apps*100:.1f}%)")
        self.log(f"Applications with approved ORIG submissions: {apps_with_approved_submissions} ({apps_with_approved_submissions/total_apps*100:.1f}%)")
        self.log(f"Applications with approval letters: {apps_with_letter_docs} ({apps_with_letter_docs/total_apps*100:.1f}%)")
        self.log("")
        
        self.log(f"REASONS FOR MISSING APPROVAL LETTERS (ANDAs after {min_year}):")
        self.log(f"Has ORIG+Approved with letters: {letter_reasons['has_orig_approved']} ({letter_reasons['has_orig_approved']/total_apps*100:.1f}%)")
        self.log(f"Has ORIG but no documents: {letter_reasons['has_orig_no_docs']} ({letter_reasons['has_orig_no_docs']/total_apps*100:.1f}%)")
        self.log(f"Has documents but no letters: {letter_reasons['has_docs_no_letters']} ({letter_reasons['has_docs_no_letters']/total_apps*100:.1f}%)")
        self.log(f"No ORIG submissions: {letter_reasons['no_orig_submissions']} ({letter_reasons['no_orig_submissions']/total_apps*100:.1f}%)")
        self.log(f"No submissions at all: {letter_reasons['no_submissions']} ({letter_reasons['no_submissions']/total_apps*100:.1f}%)")
        self.log("")
        
        self.log(f"CONCLUSION FOR ANDAs AFTER {min_year}:")
        if min_year >= 2000:
            self.log("For modern ANDAs (2000+), we expect better documentation availability:")
        else:
            self.log("For ANDAs after 1982, including older applications:")
        self.log("1. Digital documentation should be more complete for newer ANDAs")
        self.log("2. API coverage varies by historical period")
        self.log("3. Document availability depends on FDA digitization efforts")
        self.log("4. Newer ANDAs more likely to have complete submission records")
    
    def save_to_file(self, filename: str):
        """Save analysis to text file."""
        with open(filename, 'w', encoding='utf-8') as f:
            for line in self.output_lines:
                f.write(line + '\n')
        
        self.log(f"Analysis saved to: {filename}")
    
    def clear_output(self):
        """Clear output lines for new analysis."""
        self.output_lines = []

def main():
    """Main function to run date-filtered ANDA analyses."""
    analyzer = ANDADateFilteredAnalyzer(API_KEY)
    
    # Analysis 1: ANDAs after 1982
    print("Starting analysis of ANDAs after 1982...")
    analyzer.analyze_date_filtered_andas(min_year=1982, target_count=100)
    analyzer.save_to_file("anda_analysis_after_1982.txt")
    
    # Clear output and start new analysis
    analyzer.clear_output()
    print("\n" + "="*80)
    print("Starting analysis of ANDAs after 2000...")
    
    # Analysis 2: ANDAs after 2000 - 1000 applications (API limit)
    analyzer.analyze_date_filtered_andas(min_year=2000, target_count=1000)
    analyzer.save_to_file("anda_analysis_after_2000_1000.txt")

if __name__ == "__main__":
    main()