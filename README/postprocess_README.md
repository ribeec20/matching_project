# postprocess.py - Validation and Monopoly Time Calculation

## Overview
This module handles **post-matching validation and analysis**, including FDA API integration, PDF company validation, monopoly time calculation, and result summarization. It bridges the matching algorithm with real-world evidence from FDA data.

## Purpose
**Validation & Analysis**: Validate NDA-ANDA matches using FDA approval letter PDFs and company name matching, calculate monopoly times, identify NDAs without ANDA competition, and generate diagnostic summaries.

## Key Components

---

## 1. FDA API Integration

### `get_api_submissions(anda_objects: List) -> Dict[str, Optional[str]]`
**Purpose**: Query FDA API to get approval letter PDF URLs for ANDA applications.

**Logic**:
1. Create `DrugsAPI` client instance
2. Call `api_client.get_multiple_anda_pdfs(anda_objects, rate_limit_delay=0.5)`
3. Returns dictionary: `{anda_number: pdf_url or None}`

**Input Format**:
- List of objects with `get_anda_number()` method
- Example: `[SimpleANDA("074830"), SimpleANDA("076703"), ...]`

**Rate Limiting**: 0.5 seconds between requests (max 120 requests/minute)

**Example**:
```python
anda_objects = [SimpleANDA("074830"), SimpleANDA("076703")]
pdf_urls = get_api_submissions(anda_objects)
# → {"074830": "http://www.accessdata.fda.gov/.../74830ltr.pdf", "076703": None}
```

---

## 2. Company Data Retrieval

### `get_nda_companies_from_orange_book(nda_numbers: List[str], orange_book_clean: pd.DataFrame) -> Dict[str, List[str]]`
**Purpose**: Extract all company names for each NDA from Orange Book.

**Logic**:
1. Filter Orange Book for NDAs only (`Appl_Type == 'N'`)
2. For each NDA number:
   - Find all rows with matching Appl_No
   - Extract unique company names from `Applicant` column
   - Handle null values
   - Store as list
3. Log warning if no companies found
4. Return dictionary mapping NDA → company list

**Example**:
```python
nda_companies = get_nda_companies_from_orange_book(
    ["021513", "020702"], 
    orange_book_clean
)
# → {
#     "021513": ["PFIZER INC", "PARKE-DAVIS DIV"],
#     "020702": ["BRISTOL-MYERS SQUIBB COMPANY"]
# }
```

**Why Multiple Companies**: NDAs can have multiple applicants (parent companies, subsidiaries, transfers)

### `get_nda_companies_from_main_table(nda_numbers: List[str], main_table_clean: pd.DataFrame, orange_book_clean: pd.DataFrame) -> Dict[str, List[str]]`
**Purpose**: Get companies for NDAs in main table, using Orange Book as source but main table as filter.

**Logic**:
1. Get set of NDA numbers from main table
2. Filter Orange Book for NDAs only
3. For each NDA number:
   - **Check if NDA is in main table** (important filter)
   - If yes: Extract companies from Orange Book
   - If no: Skip (log info message)
4. Return dictionary with only main table NDAs

**Why This Version**: Ensures we only get companies for NDAs we're actually studying

**Example**:
```python
nda_companies = get_nda_companies_from_main_table(
    ["021513", "099999"],  # 099999 not in main table
    main_table_clean,
    orange_book_clean
)
# → {
#     "021513": ["PFIZER INC"],
#     "099999": []  # Skipped, not in main table
# }
```

---

## 3. PDF URL Extraction

### `find_working_anda_pdf_url(anda_num: str, year: int) -> Optional[str]`
**Purpose**: Try multiple URL patterns to find accessible ANDA approval letter PDF.

**Logic** (tries 5 patterns):

1. **Standard pattern**: `{year}/{anda_num}ltr.pdf`
2. **s000 suffix**: `{year}/{anda_num}s000ltr.pdf`
3. **Underscore variant**: `{year}/{anda_num}s000_ltr.pdf`
4. **Orig1s000 (capitalized)**: `{year}/{anda_num}Orig1s000ltr.pdf`
5. **orig1s000 (lowercase)**: `{year}/{anda_num}orig1s000ltr.pdf`

**For Each Pattern**:
1. Make HTTP HEAD request (fast, no download)
2. Check status code == 200
3. Verify URL contains "http://www.accessdata.fda.gov"
4. Verify URL contains "/appletter/"
5. If all checks pass → return URL
6. If any check fails → try next pattern

**Validation Logic**:
```python
if (response.status_code == 200 and 
    'http://www.accessdata.fda.gov' in response.url and 
    '/appletter/' in response.url):
    return url  # Valid FDA URL
```

**Why Multiple Patterns**: FDA uses inconsistent naming conventions across years

**Example**:
```python
url = find_working_anda_pdf_url("074830", 1999)
# Tries:
# 1. http://www.accessdata.fda.gov/.../1999/74830ltr.pdf ✗ 404
# 2. http://www.accessdata.fda.gov/.../1999/74830s000ltr.pdf ✓ 200
# → Returns pattern 2
```

### `extract_anda_pdf_urls(anda_matches: pd.DataFrame, test_urls: bool = True) -> Dict[str, str]`
**Purpose**: Build PDF URL dictionary for all ANDAs in match dataset.

**Logic**:
1. Extract unique ANDA numbers from `ANDA_Appl_No` column
2. For each ANDA:
   - Get approval year from `ANDA_Approval_Date_Date`
   - If `test_urls=True`: Call `find_working_anda_pdf_url()` to test patterns
   - If `test_urls=False`: Use default pattern without testing
   - Store working URL in dictionary
3. Return `{anda_number: pdf_url}` dictionary

**test_urls Parameter**:
- `True`: Slower but more reliable (tests each URL)
- `False`: Faster but may include broken URLs

**Example**:
```python
pdf_urls = extract_anda_pdf_urls(anda_matches, test_urls=True)
# → {"074830": "http://...74830s000ltr.pdf", "076703": "http://...76703ltr.pdf"}
```

---

## 4. PDF Company Extraction

### `extract_company_references_from_pdfs(anda_pdf_urls: Dict[str, str]) -> Dict[str, Optional[str]]`
**Purpose**: Extract bioequivalence company reference text from ANDA approval letter PDFs.

**Logic**:
1. Create `BatchPDFExtractor` with 1.0 second rate limit
2. Call `batch_extractor.extract_companies_from_andas(anda_pdf_urls)`
3. Returns `{anda_number: reference_text or None}`

**Integration**: Uses `extract_from_pdf.py` module for actual PDF parsing

**Example**:
```python
company_refs = extract_company_references_from_pdfs(pdf_urls)
# → {
#     "074830": "The Office of Bioequivalence has determined that your product is bioequivalent to Glucophage Tablets of Bristol-Myers Squibb Company.",
#     "076703": None  # PDF not parseable
# }
```

---

## 5. Company Matching

### `calculate_text_similarity(company_name: str, reference_text: str) -> float`
**Purpose**: Calculate similarity score between company name and reference text.

**Logic**:

**For Empty Inputs**:
```python
if not company_name or not reference_text:
    return 0.0
```

**For Long Company Names** (>3 chars per word):
```python
company_words = [word for word in company_upper.split() if len(word) > 3]
matched_words = sum(1 for word in company_words if word in text_upper)
return matched_words / len(company_words)  # Percentage match
```
- Filters out short words like "INC", "LLC", "CO"
- Counts significant word matches
- Returns 0.0 to 1.0 score

**For Short Company Names**:
```python
return 1.0 if company_upper in text_upper else 0.0
```
- Uses exact substring matching
- All-or-nothing score

**Examples**:
```python
calculate_text_similarity(
    "BRISTOL-MYERS SQUIBB COMPANY",
    "bioequivalent to Glucophage of Bristol-Myers Squibb Company"
)
# → 1.0 (3/3 words: BRISTOL, MYERS, SQUIBB all present)

calculate_text_similarity(
    "PFIZER INC",
    "bioequivalent to Lipitor of Pfizer Inc"
)
# → 1.0 (1/1 word: PFIZER present, INC ignored as short)

calculate_text_similarity(
    "NOVARTIS",
    "bioequivalent to product of Pfizer Inc"
)
# → 0.0 (exact match required for single-word name, not found)
```

---

## 6. Match Validation

### `validate_company_matches(nda_companies, company_references, anda_matches, similarity_threshold=0.9) -> Tuple[pd.DataFrame, pd.DataFrame]`
**Purpose**: Core validation logic - validate NDA-ANDA matches using company name similarity.

**Conservative Approach**: Only rejects matches with **confirmed conflicts** (low similarity). Missing data → keep match.

**Logic**:

**For Each Match**:
```python
for _, match_row in anda_matches.iterrows():
    nda_num = str(match_row['NDA_Appl_No'])
    anda_num = str(match_row['ANDA_Appl_No'])
    
    nda_company_list = nda_companies.get(nda_num, [])
    anda_ref_text = company_references.get(anda_num)
```

**Validation Decision Tree**:

**1. If company found (≥90% similarity)**:
```python
if company_found:
    validation_status = 'Validated'
    validated_matches.append(match_row_copy)
    logger.info(f"✓ Validated: NDA {nda_num} - ANDA {anda_num} (Company: {matched_company}, Similarity: {best_similarity:.2f})")
```

**2. If we have both PDF text AND NDA companies, but NO match**:
```python
elif anda_ref_text and nda_company_list:
    max_similarity = calculate_max_similarity(nda_company_list, anda_ref_text)
    
    if max_similarity < 0.2:  # Very low similarity
        validation_status = 'Rejected'
        rejected_matches.append(match_row_copy)
        logger.warning(f"✗ Rejected: NDA {nda_num} - ANDA {anda_num} (Low company similarity: {max_similarity:.2f})")
    else:  # Marginal similarity (0.2-0.9)
        validation_status = 'Unknown'
        validated_matches.append(match_row_copy)  # Keep conservatively
        logger.info(f"? Keeping: NDA {nda_num} - ANDA {anda_num} (Marginal similarity: {max_similarity:.2f})")
```

**3. If missing data (no PDF or no companies)**:
```python
else:
    validation_status = 'Unknown'
    validated_matches.append(match_row_copy)  # Keep conservatively
    logger.info(f"? Keeping: NDA {nda_num} - ANDA {anda_num} (Insufficient data for validation)")
```

**Rejection Threshold**: Only reject if similarity < 0.2 (20%)
- 90%+ similarity → Validated (strong match)
- 20-90% similarity → Keep (uncertain but possible)
- <20% similarity → Rejected (clear mismatch)

**Added Columns**:
- `NDA_Companies`: Pipe-separated company list
- `Company_Match_Found`: Boolean
- `Matched_Company`: Which company matched
- `Company_Match_Similarity`: 0.0-1.0 score
- `Validation_Status`: "Validated", "Rejected", or "Unknown"

**Returns**: `(validated_df, rejected_df)`

---

## 7. Main Validation Pipeline

### `nda_anda_company_validation(match_data, orange_book_clean, main_table_clean, max_andas_to_process=None) -> Tuple[pd.DataFrame, pd.DataFrame, Dict]`
**Purpose**: Complete end-to-end validation pipeline with FDA API and PDF extraction.

**Steps**:

**Step 1: Get Unique NDAs**
```python
nda_numbers = match_data.anda_matches['NDA_Appl_No'].dropna().astype(str).unique().tolist()
logger.info(f"Found {len(nda_numbers)} unique NDAs to validate")
```

**Step 2: Get NDA Companies from Main Table**
```python
nda_companies = get_nda_companies_from_main_table(nda_numbers, main_table_clean, orange_book_clean)
```
- Uses main table as filter, Orange Book as company source

**Step 3: Limit Processing for Testing (Optional)**
```python
if max_andas_to_process:
    unique_andas = anda_matches_to_process['ANDA_Appl_No'].dropna().unique()
    limited_andas = unique_andas[:max_andas_to_process]
    anda_matches_to_process = anda_matches_to_process[
        anda_matches_to_process['ANDA_Appl_No'].isin(limited_andas)
    ]
```
- Example: `max_andas_to_process=10` limits to first 10 ANDAs

**Step 4: Query FDA API for PDF URLs**
```python
class SimpleANDA:
    def __init__(self, anda_num):
        self.anda_num = str(anda_num)
    def get_anda_number(self):
        return self.anda_num

unique_anda_numbers = anda_matches_to_process['ANDA_Appl_No'].dropna().unique()
anda_objects = [SimpleANDA(anda_num) for anda_num in unique_anda_numbers]
anda_pdf_urls = get_api_submissions(anda_objects)
anda_pdf_urls = {k: v for k, v in anda_pdf_urls.items() if v is not None}
```
- Creates simple ANDA objects for API
- Filters out None values (failed API queries)

**Step 5: Extract Company References from PDFs**
```python
company_references = extract_company_references_from_pdfs(anda_pdf_urls)
```

**Step 6: Validate Matches (90% Threshold)**
```python
validated_matches, rejected_matches = validate_company_matches(
    nda_companies, company_references, anda_matches_to_process, similarity_threshold=0.9
)
```

**Step 7: Compile Validation Details**
```python
validation_details = {
    'nda_companies': nda_companies,
    'anda_pdf_urls': anda_pdf_urls,
    'company_references': company_references,
    'total_matches_processed': len(anda_matches_to_process),
    'validated_count': len(validated_matches),
    'rejected_count': len(rejected_matches)
}
```

**Returns**: `(validated_matches, rejected_matches, validation_details)`

**Logging Example**:
```
INFO - Starting NDA-ANDA company validation process...
INFO - Found 306 unique NDAs to validate
INFO - Retrieving NDA companies for main table NDAs...
INFO - Querying FDA API for ANDA approval letter URLs...
INFO - Found 1203 ANDA PDF URLs from FDA API
INFO - Extracting company references from ANDA approval letters...
INFO - Extracting company references from 1203 ANDA approval letters...
INFO - Processing ANDA 074830 (1/1203): http://www.accessdata.fda.gov/.../74830ltr.pdf
INFO - ✓ Successfully extracted reference text for ANDA 074830
...
INFO - Extraction complete: 485/1203 company references found (40.3%)
INFO - Validating NDA-ANDA matches based on company references...
✓ Validated: NDA 021513 - ANDA 074830 (Company: PFIZER INC, Similarity: 1.00)
? Keeping: NDA 020702 - ANDA 076703 (No PDF text available)
✗ Rejected: NDA 018936 - ANDA 089234 (Low company similarity: 0.10)
...
INFO - Validation complete: 2340 validated, 203 rejected
```

---

## 8. MatchData Filtering

### `create_validated_match_data(original_match_data, validated_matches) -> MatchData`
**Purpose**: Create new MatchData object containing only validated matches.

**Logic**:
```python
validated_match_data = MatchData(
    study_ndas=original_match_data.study_ndas,              # Unchanged
    study_ndas_strength=original_match_data.study_ndas_strength,  # Unchanged
    ndas_ob=original_match_data.ndas_ob,                    # Unchanged
    andas_ob=original_match_data.andas_ob,                  # Unchanged
    study_ndas_final=original_match_data.study_ndas_final,  # Unchanged
    candidates=original_match_data.candidates,              # Unchanged
    anda_matches=validated_matches,                         # FILTERED
    nda_summary=original_match_data.nda_summary,            # Unchanged
    ob_nda_first=original_match_data.ob_nda_first,          # Unchanged
    date_check=original_match_data.date_check,              # Unchanged
)
```

**Key Change**: Only `anda_matches` is replaced with validated subset

---

## 9. Monopoly Time Calculation

### `calculate_nda_monopoly_times_with_validation(match_data, validation_status_map=None) -> pd.DataFrame`
**Purpose**: Calculate monopoly periods at NDA level with validation tracking.

**Logic**:

**Step 1: Get NDA Summary**
```python
nda_summary = match_data.nda_summary.copy()
```

**Step 2: Filter Valid Matches**
```python
valid_matches = match_data.anda_matches.copy()
valid_matches = valid_matches.dropna(subset=["ANDA_Approval_Date_Date"])

# Merge with NDA dates
valid_matches = valid_matches.merge(
    nda_summary[["NDA_Appl_No", "NDA_Approval_Date_Date"]], 
    on="NDA_Appl_No", 
    how="left"
)

# Filter: ANDA approved AFTER NDA
valid_matches = valid_matches[
    valid_matches["ANDA_Approval_Date_Date"] > valid_matches["NDA_Approval_Date_Date"]
]
```

**Step 3: Find Earliest ANDA per NDA**
```python
earliest_anda_per_nda = (
    valid_matches.groupby("NDA_Appl_No")["ANDA_Approval_Date_Date"]
    .min()
    .rename("Earliest_ANDA_Date")
    .reset_index()
)
```

**Step 4: Count Matching ANDAs**
```python
anda_details = (
    valid_matches.groupby("NDA_Appl_No")
    .agg({
        "ANDA_Appl_No": [
            lambda x: x.nunique(),  # Count
            lambda x: " | ".join(sorted(set(x.astype(str))))  # List
        ]
    })
    .reset_index()
)
anda_details.columns = ["NDA_Appl_No", "Num_Matching_ANDAs", "Matching_ANDA_List"]
```

**Step 5: Merge Back to NDA Summary**
```python
nda_monopoly = (
    nda_summary
    .merge(earliest_anda_per_nda, on="NDA_Appl_No", how="left")
    .merge(anda_details, on="NDA_Appl_No", how="left")
)
```

**Step 6: Calculate Monopoly Times**
```python
nda_monopoly["Actual_Monopoly_Days"] = (
    nda_monopoly["Earliest_ANDA_Date"] - nda_monopoly["NDA_Approval_Date_Date"]
).dt.days

nda_monopoly["Actual_Monopoly_Years"] = nda_monopoly["Actual_Monopoly_Days"] / 365.25
```

**Step 7: Compare to Granted Monopoly**
```python
def shorter_than_granted(row):
    actual = row["Actual_Monopoly_Years"]
    granted = row["NDA_MMT_Years"]
    if pd.isna(actual) or pd.isna(granted):
        return np.nan
    return bool(actual < granted)

nda_monopoly["Monopoly_Shorter_Than_Granted"] = nda_monopoly.apply(shorter_than_granted, axis=1)
```

**Output Columns**:
- `NDA_Appl_No`: NDA number
- `NDA_Approval_Date_Date`: NDA approval datetime
- `NDA_Ingredient`, `NDA_Applicant`: NDA details
- `NDA_MMT_Years`: Granted monopoly period
- `Earliest_ANDA_Date`: First ANDA approval datetime
- `Num_Matching_ANDAs`: Count of validated ANDAs
- `Matching_ANDA_List`: Pipe-separated ANDA numbers
- `Actual_Monopoly_Days`: Days from NDA to first ANDA
- `Actual_Monopoly_Years`: Years from NDA to first ANDA
- `Monopoly_Shorter_Than_Granted`: Boolean comparison
- `Validation_Status`: From validation process

---

## 10. Diagnostic Outputs

### `build_postprocess_outputs(match_data) -> Dict[str, pd.DataFrame]`
**Purpose**: Generate all diagnostic DataFrames for analysis.

**Outputs**:
1. **strength_summary**: How well strengths matched
2. **date_summary**: Date alignment between sources
3. **nda_monopoly_times**: Full monopoly time analysis
4. **ndas_no_anda**: NDAs without any ANDA matches

**Usage**:
```python
outputs = build_postprocess_outputs(match_data)
plot_monopoly_scatter(outputs["nda_monopoly_times"])
```

### `display_postprocess_summary(outputs) -> None`
**Purpose**: Print human-readable summary of results.

**Example Output**:
```
Strength matching summary:
   rows  strength_match_true  strength_match_false
0  1157                 1084                    73

NDA approval date comparison summary:
   ndas  both_dates  exact_same  diff_nonzero  na_in_either  median_diff  p90_diff
0   619         590         567            23            29          0.0      12.0

NDA monopoly time summary:
   Total NDAs: 619
   NDAs with ANDA matches: 306
   NDAs with calculated monopoly times: 306

NDAs with no matched ANDAs (first 10):
  NDA_Appl_No         NDA_Ingredient NDA_Approval_Date
0      018662  ACETAMINOPHEN; ASPIRIN        1989-12-29
1      019540         ACYCLOVIR SODIUM        1991-08-29
...
```

---

## Integration Points

### Input (from match.py)
```python
match_data = match_ndas_to_andas(main_clean, orange_clean)
```

### Validation
```python
validated_matches, rejected_matches, details = nda_anda_company_validation(
    match_data, orange_book_clean, main_table_clean, max_andas_to_process=None
)
```

### Output (to monopoly_time.py)
```python
outputs = build_postprocess_outputs(match_data)
plot_monopoly_scatter(outputs["nda_monopoly_times"])
```

## Dependencies

### Required Libraries
- **pandas**: DataFrame operations
- **numpy**: NaN handling
- **requests**: PDF URL testing
- **logging**: Progress tracking

### Internal Dependencies
- **match.MatchData**: Data structure from matching
- **extract_from_pdf.BatchPDFExtractor**: PDF parsing
- **drugs_api.DrugsAPI**: FDA API client

## Performance Characteristics

### FDA API Queries
- **Speed**: 0.5 seconds per ANDA (rate limited)
- **100 ANDAs**: ~50 seconds
- **1000 ANDAs**: ~8-10 minutes

### PDF Extraction
- **Speed**: 1-3 seconds per PDF (download + parse)
- **100 PDFs**: ~2-5 minutes
- **1000 PDFs**: ~20-50 minutes

### Overall Pipeline
- **2996 ANDAs**: ~1-2 hours for complete validation

## Usage Example

```python
from match import match_ndas_to_andas
from postprocess import nda_anda_company_validation, build_postprocess_outputs

# Match NDAs to ANDAs
match_data = match_ndas_to_andas(main_clean, orange_clean)

# Validate with company matching
validated, rejected, details = nda_anda_company_validation(
    match_data, orange_clean, main_clean
)

# Create filtered match data
from postprocess import create_validated_match_data
validated_match_data = create_validated_match_data(match_data, validated)

# Build diagnostic outputs
outputs = build_postprocess_outputs(validated_match_data)

# Display summary
from postprocess import display_postprocess_summary
display_postprocess_summary(outputs)
```
