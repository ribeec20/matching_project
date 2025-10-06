# extract_from_pdf.py - PDF Company Reference Extraction

## Overview
This module extracts company reference information from FDA approval letter PDFs to validate NDA-ANDA matches. It parses PDF text to find bioequivalence determination statements that contain the reference drug company name.

## Purpose
**Company Validation**: Confirms that ANDAs reference the correct NDA by matching company names mentioned in the ANDA approval letter against the known NDA company. This provides evidence-based validation of algorithmic NDA-ANDA matches.

## Key Components

### PDFCompanyExtractor Class
Handles extraction of company reference text from individual FDA approval letter PDFs.

#### Initialization
```python
PDFCompanyExtractor()
```
- Creates HTTP session with browser-like User-Agent
- Prepares session for multiple PDF downloads
- No API keys required (direct PDF downloads)

#### Core Methods

##### 1. `parse_pdf_from_url(pdf_url: str) -> str`
**Purpose**: Download and extract raw text from a PDF URL.

**Logic**:
1. Makes HTTP GET request to PDF URL
2. Uses 30-second timeout for slow servers
3. Loads PDF content into BytesIO stream
4. Uses PyPDF2.PdfReader to parse PDF
5. Iterates through all pages
6. Extracts text from each page
7. Combines all page text with newlines
8. Returns complete PDF text or empty string on error

**Error Handling**:
- Network failures: Logged and returns empty string
- Malformed PDFs: Caught and returns empty string
- Timeout errors: 30-second limit prevents hanging

**Example**:
```python
extractor = PDFCompanyExtractor()
text = extractor.parse_pdf_from_url("http://www.accessdata.fda.gov/drugsatfda_docs/appletter/1999/74830ltr.pdf")
# Returns: "Dear Sir or Madam: We have approved your ANDA..."
```

##### 2. `extract_reference_company(text: str) -> Optional[str]`
**Purpose**: Extract the bioequivalence determination sentence containing company name.

**Logic**:
1. Defines multiple regex patterns to match bioequivalence statements
2. Searches for patterns in order of specificity
3. Extracts matching sentence(s)
4. Normalizes whitespace (removes extra spaces/newlines)
5. Returns first successful match or None

**Pattern Strategy** (in order of priority):

**Pattern 1: Office of Bioequivalence with Company Suffix**
```regex
(The\s+Office\s+of\s+Bioequivalence[^.\n]*(?:\.[^.\n]*)*?(?:Inc\.|LLC|Corporation|Corp\.|Limited|Ltd\.)(?:[^.\n]*(?:\n(?!\n))?)*)
```
- Captures full sentence starting with "The Office of Bioequivalence"
- Continues until finding company suffix (Inc., LLC, Corp., etc.)
- Handles multi-line sentences

**Pattern 2: Division of Bioequivalence**
```regex
(The\s+Division\s+of\s+Bioequivalence\s+has\s+determined[^.\n]*(?:\.[^.\n]*)*?(?:Inc\.|LLC|Corporation|Corp\.|Limited|Ltd\.)...)
```
- Alternate FDA department name
- Same company suffix matching

**Pattern 3: Fallback - Full Bioequivalence Statement**
```regex
(The\s+(?:Office|Division)\s+of\s+Bioequivalence[^\n]*(?:\n(?!\n)[^\n]*)*)
```
- Captures complete bioequivalence statement
- Used when company suffix not found
- Ensures some text is extracted

**Pattern 4: Direct Bioequivalence Statement**
```regex
([^.\n]*bioequivalent\s+and\s+therapeutically\s+equivalent\s+to\s+the\s+reference\s+listed\s+drug[^\n]*...)
```
- Searches for key phrase
- Useful for non-standard letter formats

**Example Extracted Text**:
```
"The Office of Bioequivalence has determined that your ANDA product is bioequivalent 
and therapeutically equivalent to the reference listed drug, Glucophage Tablets, 
500 mg, 850 mg, and 1000 mg, of Bristol-Myers Squibb Company."
```

**Why This Works**:
- FDA approval letters follow standardized formats
- Bioequivalence determination always mentions reference drug
- Company name appears after reference drug name
- Company suffixes (Inc., LLC, Corp.) are reliable markers

##### 3. `get_company_reference(pdf_url: str) -> Optional[str]`
**Purpose**: Complete pipeline to extract company reference from PDF URL.

**Logic**:
1. Calls `parse_pdf_from_url()` to get text
2. If text extraction fails, returns None
3. Calls `extract_reference_company()` on text
4. Returns extracted reference or None

**Return Values**:
- **Success**: Sentence containing company reference
- **Failure**: None (PDF not accessible or pattern not found)

### BatchPDFExtractor Class
Processes multiple ANDA PDFs in batch with rate limiting.

#### Initialization
```python
BatchPDFExtractor(rate_limit_delay: float = 1.0)
```
- **rate_limit_delay**: Seconds to wait between PDF downloads (default 1.0)
- Creates internal `PDFCompanyExtractor` instance

#### Core Method

##### `extract_companies_from_andas(anda_pdf_urls: Dict[str, str]) -> Dict[str, Optional[str]]`
**Purpose**: Extract company references from multiple ANDA approval letters.

**Logic**:
1. Accepts dictionary of ANDA number → PDF URL
2. Logs total count for progress tracking
3. For each ANDA:
   - Logs current progress (e.g., "Processing 5/100")
   - Calls `get_company_reference()` with PDF URL
   - Stores result (text or None) in results dictionary
   - Logs success/failure
   - Implements rate limiting delay
4. Calculates and logs summary statistics
5. Returns complete results dictionary

**Rate Limiting**:
- Prevents overwhelming FDA servers
- Default 1.0 second = max 60 PDFs/minute
- Adjustable based on server response

**Progress Logging**:
```
INFO - Extracting company references from 2996 ANDA approval letters...
INFO - Processing ANDA 074830 (1/2996): http://www.accessdata.fda.gov/.../74830ltr.pdf
INFO - ✓ Successfully extracted reference text for ANDA 074830
INFO - Processing ANDA 076703 (2/2996): http://www.accessdata.fda.gov/.../76703ltr.pdf
INFO - ✗ No company reference text found for ANDA 076703
...
INFO - Extraction complete: 1203/2996 company references found (40.2%)
```

**Error Handling**:
- Individual PDF failures don't stop batch processing
- Errors logged per ANDA
- Failed extractions marked as None in results

## Text Processing Techniques

### Whitespace Normalization
```python
sentence = re.sub(r'\s+', ' ', sentence)
```
- Collapses multiple spaces, tabs, newlines into single space
- Ensures consistent text for comparison

### Multi-line Pattern Matching
```regex
(?:\n(?!\n))?
```
- Allows patterns to span line breaks
- Stops at paragraph breaks (double newlines)
- Critical for PDF text with line wrapping

### Case-Insensitive Matching
```python
re.findall(pattern, text, re.IGNORECASE | re.DOTALL)
```
- **IGNORECASE**: Matches regardless of capitalization
- **DOTALL**: `.` matches newline characters

## Integration in Validation Pipeline

### Step 1: Get PDF URLs (from drugs_api.py)
```python
anda_pdf_urls = {
    '074830': 'http://www.accessdata.fda.gov/.../74830ltr.pdf',
    '076703': 'http://www.accessdata.fda.gov/.../76703ltr.pdf'
}
```

### Step 2: Extract Company References
```python
from extract_from_pdf import BatchPDFExtractor

extractor = BatchPDFExtractor(rate_limit_delay=1.0)
company_references = extractor.extract_companies_from_andas(anda_pdf_urls)
```

### Step 3: Results Used for Validation
```python
{
    '074830': 'The Office of Bioequivalence has determined that your product is bioequivalent to Glucophage Tablets of Bristol-Myers Squibb Company.',
    '076703': None  # Extraction failed
}
```

### Step 4: Company Matching (in postprocess.py)
```python
# Check if NDA company appears in ANDA reference text
if 'BRISTOL-MYERS SQUIBB' in company_references['074830'].upper():
    # Match validated ✓
```

## Performance Characteristics

### Extraction Speed
- **Per PDF**: ~1-3 seconds (1s download + parsing)
- **100 PDFs**: ~2-5 minutes
- **1000 PDFs**: ~20-50 minutes

### Success Rates
- **PDF Accessible**: 40-60% (depends on FDA API success)
- **Text Extractable**: 90-95% (of accessible PDFs)
- **Pattern Matched**: 75-85% (of extractable text)
- **Overall Success**: ~30-50% of total ANDAs

### Memory Usage
- Each PDF: ~500KB - 2MB
- Processed in streaming fashion (no accumulation)
- Low memory footprint even for large batches

## Common Failure Modes

### 1. PDF Not Accessible (404, Timeout)
```
Error parsing PDF http://...74830ltr.pdf: HTTPError 404
```
**Solution**: FDA API should provide valid URLs; fallback patterns attempted

### 2. PDF Parsing Fails (Corrupted, Encrypted)
```
Error parsing PDF: PdfReadError: EOF marker not found
```
**Solution**: Logged as failure, continues with next PDF

### 3. Pattern Not Matched (Non-standard Format)
```
✗ No company reference text found for ANDA 076703
```
**Reasons**:
- Older approval letters with different format
- Scanned images instead of text PDFs
- Redacted company names
- Non-standard bioequivalence statements

**Solution**: Conservative validation keeps matches without positive evidence of mismatch

## Example Extracted Patterns

### Standard Format
```
"The Office of Bioequivalence has determined that your ANDA product is 
bioequivalent and therapeutically equivalent to the reference listed drug, 
Glucophage Tablets, 500 mg, 850 mg, and 1000 mg, of Bristol-Myers Squibb Company."
```

### Variation with Division
```
"The Division of Bioequivalence has determined that the drug product referenced 
in this ANDA is bioequivalent to Lipitor Tablets, 10 mg, 20 mg, 40 mg, and 80 mg, 
of Pfizer Inc."
```

### Short Form
```
"Your drug product is bioequivalent and therapeutically equivalent to the 
reference listed drug Zocor Tablets of Merck & Co., Inc."
```

## Dependencies

### Required Libraries
- **requests**: HTTP client for PDF downloads
- **PyPDF2**: PDF parsing and text extraction
- **re**: Regular expression pattern matching
- **time**: Rate limiting delays
- **logging**: Progress tracking and error reporting
- **typing**: Type hints
- **io.BytesIO**: In-memory PDF stream handling

### Installation
```bash
pip install requests PyPDF2
```

## Logging Configuration
```python
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
```

**Levels Used**:
- **INFO**: Progress updates, success/failure counts, summaries
- **WARNING**: Missing references, pattern match failures
- **ERROR**: PDF parsing errors, network failures
- **DEBUG**: Detailed URL fetching (if enabled)

## Future Enhancements

1. **OCR Support**: Extract text from scanned/image PDFs
2. **Fuzzy Company Matching**: Handle spelling variations
3. **Multi-language Support**: Non-English approval letters
4. **Caching**: Store extracted text to avoid re-parsing
5. **Parallel Processing**: Download multiple PDFs simultaneously
6. **Alternative Patterns**: Add more regex patterns for edge cases
7. **Company Name Database**: Cross-reference against known variations

## Usage Example

```python
from extract_from_pdf import BatchPDFExtractor

# Initialize batch extractor
extractor = BatchPDFExtractor(rate_limit_delay=1.0)

# PDF URLs from FDA API
anda_pdfs = {
    '074830': 'http://www.accessdata.fda.gov/drugsatfda_docs/appletter/1999/74830ltr.pdf',
    '076703': 'http://www.accessdata.fda.gov/drugsatfda_docs/appletter/2005/076703ltr.pdf'
}

# Extract company references
company_refs = extractor.extract_companies_from_andas(anda_pdfs)

# Results
for anda_num, ref_text in company_refs.items():
    if ref_text:
        print(f"ANDA {anda_num}: {ref_text[:100]}...")
    else:
        print(f"ANDA {anda_num}: Extraction failed")
```

## Integration Point
This module is called by `postprocess.py` in the `nda_anda_company_validation()` function as part of the overall validation pipeline.
