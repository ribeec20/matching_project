# drugs_api.py - FDA Drugs@FDA API Client

## Overview
This module provides an object-oriented interface to the FDA openFDA API for retrieving ANDA (Abbreviated New Drug Application) data and approval letter PDF URLs. It handles API authentication, rate limiting, and extracts approval letter URLs from the FDA's complex submission structure.

## Purpose
The primary purpose is to **validate NDA-ANDA matches** by retrieving FDA approval letter PDFs that contain company reference information. This allows the pipeline to confirm that the company listed in the ANDA approval letter matches the expected NDA company.

## Key Components

### DrugsAPI Class
The main client class for interacting with the FDA Drugs@FDA API.

#### Initialization
```python
DrugsAPI(api_key: str = API_KEY, base_url: str = BASE_URL, rate_limit_delay: float = 0.5)
```
- **api_key**: FDA API authentication key (default provided)
- **base_url**: FDA drug API endpoint (`https://api.fda.gov/drug`)
- **rate_limit_delay**: Delay between requests to respect FDA rate limits (default 0.5 seconds) MUST HAVE SOME DELAY

#### Core Methods

##### 1. `search_application(application_number: str, limit: int = 1) -> Optional[Dict]`
**Purpose**: Search for a specific drug application by application number.

**Logic**:
1. Constructs API query URL with application number
2. Adds API key and search parameters
3. Makes HTTP GET request with timeout protection
4. Implements rate limiting after successful request
5. Returns JSON response or None on failure

**Example**:
```python
api = DrugsAPI()
result = api.search_application("ANDA074830")
```

##### 2. `get_anda_data(anda_number: str) -> Optional[Dict]`
**Purpose**: Retrieve complete data for a specific ANDA application.

**Logic**:
1. Ensures ANDA number has "ANDA" prefix
2. Calls `search_application()` with formatted number
3. Extracts first result from API response
4. Returns application data dictionary or None

**Returns**: Full application record including:
- Application number and type
- Sponsor/applicant information
- Submission history
- Application documents
- Product information

##### 3. `extract_pdf_urls_from_submission(submission: Dict) -> List[str]`
**Purpose**: Extract approval letter PDF URLs from a submission's documents.

**Logic**:
1. Accesses `application_docs` list in submission
2. Filters for documents with `/appletter/` in URL path
3. Ensures URL ends with `.pdf`
4. Collects all matching PDF URLs

**URL Pattern Recognition**:
- Looks for: `http://www.accessdata.fda.gov/drugsatfda_docs/appletter/YEAR/ANDANUMBERltr.pdf`
- Common patterns:
  - `{anda}ltr.pdf`
  - `{anda}s000ltr.pdf`
  - `{anda}Orig1s000ltr.pdf`

##### 4. `get_anda_approval_letter_url(anda_number: str) -> Optional[str]`
**Purpose**: Find the approval letter PDF URL for an ANDA.

**Logic**:
1. Retrieves full ANDA data via `get_anda_data()`
2. Iterates through all submissions in application history
3. For each submission, extracts PDF URLs
4. Returns first approval letter PDF found
5. Logs success/failure for debugging

**Search Strategy**:
- Searches through ALL submissions (original + supplements)
- Prioritizes documents with `/appletter/` path
- Returns first valid PDF URL found

##### 5. `get_multiple_anda_pdfs(anda_objects: List, rate_limit_delay: float = None) -> Dict[str, Optional[str]]`
**Purpose**: Batch process multiple ANDAs to retrieve their approval letter URLs.

**Logic**:
1. Accepts list of ANDA objects with `get_anda_number()` method
2. Iterates through each ANDA
3. Calls `get_anda_approval_letter_url()` for each
4. Implements rate limiting between requests
5. Logs progress with counters (e.g., "Processing 5/100")
6. Returns dictionary mapping ANDA number → PDF URL

**Rate Limiting**:
- Default: 0.5 seconds between requests
- Configurable via parameter
- Prevents API throttling/blocking

**Output**:
```python
{
    '074830': 'http://www.accessdata.fda.gov/drugsatfda_docs/appletter/1999/74830ltr.pdf',
    '076703': 'http://www.accessdata.fda.gov/drugsatfda_docs/appletter/2005/076703ltr.pdf',
    '091496': None  # Not found
}
```

## API Configuration

### Authentication
- **API Key**: `VVwIr3zalK3V1THgAW2mym0DndMnlMBw1oBegvg6`
- **Base URL**: `https://api.fda.gov/drug`

### Rate Limiting
The FDA API has rate limits:
- **With API Key**: 240 requests per minute, 120,000 per day
- **Without API Key**: 40 requests per minute, 1,000 per day

This module implements conservative rate limiting (0.5s delay = 120 requests/minute) to stay well below limits.

## Error Handling

### Request Failures
- Network errors: Caught and logged, returns None
- Timeout errors: 30-second timeout per request
- HTTP errors: Status codes checked, failures logged

### Missing Data
- Application not found: Returns None
- No approval letters: Returns None with debug log
- Malformed responses: Handled gracefully

## Logging
Uses Python's `logging` module at INFO level:
- **INFO**: Progress updates, success/failure counts
- **DEBUG**: Detailed request/response information, missing PDFs

## Usage in Pipeline

### Step 1: Create API Client
```python
api_client = DrugsAPI()
```

### Step 2: Batch Query ANDAs
```python
from postprocess import get_api_submissions

anda_pdf_urls = get_api_submissions(anda_objects)
# Returns: {'074830': 'http://...', '076703': 'http://...', ...}
```

### Step 3: PDF URLs Used for Validation
The extracted URLs are passed to `extract_from_pdf.py` for company text extraction.

## Integration Points

### Called By
- `postprocess.py` → `get_api_submissions()` → `DrugsAPI.get_multiple_anda_pdfs()`

### Calls To
- FDA openFDA API (external)

### Data Flow
```
ANDAs → DrugsAPI → FDA API → JSON Response → PDF URLs → PDF Extractor
```

## Performance Characteristics

### API Query Time
- **Per ANDA**: ~0.5-1.0 seconds (with rate limiting)
- **100 ANDAs**: ~1-2 minutes
- **1000 ANDAs**: ~10-20 minutes

### Success Rate
- Typical: 40-60% of ANDAs have discoverable approval letters
- Older ANDAs (pre-2000): Lower success rate
- Recent ANDAs (2010+): Higher success rate

### Optimization
- Session reuse: Single HTTP session for all requests
- Batch processing: Processes multiple ANDAs in one call
- Early termination: Returns first PDF found per ANDA

## Limitations

### API Limitations
1. Not all ANDAs have digitized approval letters in FDA database
2. Some older applications lack complete submission data
3. PDF URLs may change over time (rare)

### Search Limitations
1. Requires exact ANDA number match
2. Cannot fuzzy search by company or ingredient
3. Dependent on FDA API uptime and availability

## Example Output

### Successful Query
```
INFO - Querying FDA API for 2996 ANDA applications...
INFO - Rate limit delay: 0.5 seconds between requests
INFO - ✓ Found PDF for ANDA 074830: http://www.accessdata.fda.gov/drugsatfda_docs/appletter/1999/74830ltr.pdf
INFO - ✓ Found PDF for ANDA 076703: http://www.accessdata.fda.gov/drugsatfda_docs/appletter/2005/076703ltr.pdf
...
INFO - API search complete: 1203/2996 PDFs found (40.2%)
```

### Failed Query
```
DEBUG - No API data found for ANDA 999999
DEBUG - ✗ No approval letter PDF found in API for ANDA 123456
```

## Dependencies
- **requests**: HTTP client for API calls
- **time**: Rate limiting
- **logging**: Progress tracking and debugging
- **typing**: Type hints for better code clarity

## Future Enhancements
1. Cache API responses to reduce duplicate queries
2. Parallel requests with connection pooling
3. Retry logic for transient failures
4. Support for NDA approval letters (currently ANDA-only)
5. Alternative URL pattern matching for missing PDFs
