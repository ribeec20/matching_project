"""Debug script to see what ANDA numbers are actually in the FDA API."""

import requests
import time

# FDA API Configuration
API_KEY = "VVwIr3zalK3V1THgAW2mym0DndMnlMBw1oBegvg6"
FDA_BASE_URL = "https://api.fda.gov/drug"

def debug_fda_api():
    """Debug what ANDAs are actually in the FDA API."""
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    })
    
    print("Debugging FDA API ANDA content...")
    print("=" * 50)
    
    try:
        # Use bulk API call to get ANDA applications
        url = f"{FDA_BASE_URL}/drugsfda.json"
        params = {
            "api_key": API_KEY,
            "search": "application_number:ANDA*",
            "limit": 100  # Smaller sample for debugging
        }
        
        print("Making API request...")
        response = session.get(url, params=params, timeout=60)
        response.raise_for_status()
        api_data = response.json()
        
        if 'results' not in api_data or not api_data['results']:
            print("No results found in FDA API response")
            return
        
        applications = api_data['results']
        print(f"Retrieved {len(applications)} ANDA applications from FDA API")
        print()
        
        # Show first 20 ANDAs in the API
        print("First 20 ANDAs in FDA API:")
        print("-" * 30)
        
        anda_numbers = []
        for i, app in enumerate(applications[:20]):
            app_number = app.get('application_number', 'Unknown')
            sponsor = app.get('sponsor_name', 'Unknown')
            
            # Extract ANDA number
            if app_number.startswith('ANDA'):
                anda_number = app_number[4:]  # Remove 'ANDA' prefix
                anda_numbers.append(anda_number)
                
                # Check for submissions and documents
                submissions = app.get('submissions', [])
                total_docs = 0
                pdf_docs = 0
                
                for submission in submissions:
                    docs = submission.get('application_docs', [])
                    total_docs += len(docs)
                    for doc in docs:
                        doc_url = doc.get('url', '')
                        if (doc_url and 
                            'http://www.accessdata.fda.gov' in doc_url and 
                            '/appletter/' in doc_url and
                            doc_url.lower().endswith('.pdf')):
                            pdf_docs += 1
                
                print(f"{i+1:2d}. ANDA {anda_number}: {sponsor}")
                print(f"    Submissions: {len(submissions)}, Documents: {total_docs}, PDF Letters: {pdf_docs}")
        
        print()
        print("ANDA number range analysis:")
        print("-" * 30)
        
        if anda_numbers:
            numeric_andas = []
            for anda in anda_numbers:
                try:
                    numeric_andas.append(int(anda))
                except ValueError:
                    continue
            
            if numeric_andas:
                min_anda = min(numeric_andas)
                max_anda = max(numeric_andas)
                print(f"Numeric ANDA range in API: {min_anda} to {max_anda}")
                
                # Show some specific ranges
                ranges = [
                    (200000, 999999, "200000+"),
                    (100000, 199999, "100000-199999"),
                    (75000, 99999, "75000-99999"),
                    (50000, 74999, "50000-74999"),
                    (1, 49999, "1-49999")
                ]
                
                for min_range, max_range, label in ranges:
                    count = sum(1 for n in numeric_andas if min_range <= n <= max_range)
                    if count > 0:
                        print(f"  {label}: {count} ANDAs")
        
        # Check our specific target ANDAs
        print()
        print("Checking our target ANDAs:")
        print("-" * 30)
        target_andas = ['62889', '86875', '207138', '74190', '207692']
        
        for target in target_andas:
            found = any(app.get('application_number', '') == f'ANDA{target}' for app in applications)
            print(f"ANDA {target}: {'✓ FOUND' if found else '✗ NOT FOUND'}")
        
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    debug_fda_api()