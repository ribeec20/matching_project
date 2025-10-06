# match_class.py - Object-Oriented NDA-ANDA Matching System

## Overview
This module implements an **object-oriented approach** to NDA-ANDA matching using three main classes: `NDA`, `ANDA`, and `Match`. This is the **test/experimental version** used only in `test_class_system.py`, not in production pipelines.

## Purpose
**Class-Based Architecture**: Provides an object-oriented alternative to the functional approach in `match.py`, encapsulating NDA and ANDA data with methods for validation, company matching, and monopoly time calculation.

## Key Differences: match_class.py vs match.py

| Aspect | match_class.py | match.py |
|--------|----------------|----------|
| **Paradigm** | Object-oriented (OOP) | Functional/procedural |
| **Data Structure** | NDA/ANDA/Match objects | DataFrames |
| **Usage** | test_class_system.py only | Production (dosage.py, dosage_2025.py) |
| **Validation** | Built into Match.verify_matches() | Separate postprocess.py functions |
| **Flexibility** | Individual object manipulation | Batch DataFrame operations |
| **Performance** | Slower (object overhead) | Faster (vectorized pandas) |

## Core Classes

## 1. ANDA Class

### Purpose
Encapsulates all ANDA (generic drug) data from a single Orange Book row.

### Initialization
```python
def __init__(self, row_data: pd.Series):
    self._data = row_data.copy()
    self.anda_number = str(self._data.get('Appl_No', ''))
    self.name = self.anda_number
```

**Logic**:
1. Stores entire Orange Book row as internal `_data`
2. Extracts ANDA number as primary identifier
3. Uses ANDA number as object name

### Core Getter Methods

#### Application Information
- **`get_anda_number() -> str`**: ANDA application number (e.g., "074830")
- **`get_applicant() -> str`**: Company/applicant name
- **`get_product_number() -> str`**: Product number within ANDA
- **`get_trade_name() -> str`**: Brand/trade name

#### Product Details
- **`get_ingredient() -> str`**: Active pharmaceutical ingredient
- **`get_strength() -> str`**: Strength (e.g., "10MG")
- **`get_dosage_form() -> str`**: Dosage form (e.g., "TABLET")
- **`get_route() -> str`**: Route of administration (e.g., "ORAL")

#### Approval and Status
- **`get_approval_date() -> Optional[datetime]`**: Approval date as datetime object
- **`get_approval_date_str() -> str`**: Approval date as string
- **`get_marketing_status() -> str`**: Current marketing status
- **`get_te_code() -> str`**: Therapeutic Equivalence code
- **`get_rld() -> str`**: Reference Listed Drug designation
- **`get_rs() -> str`**: Reference Standard designation

### Normalized Methods for Matching

#### `get_normalized_ingredient() -> str`
**Purpose**: Get ingredient in standardized format for matching.

**Logic**:
1. Get ingredient string
2. Apply `str_squish()` to collapse whitespace
3. Convert to uppercase
4. Return empty string if null

**Example**: `"atorvastatin calcium"` → `"ATORVASTATIN CALCIUM"`

#### `get_normalized_strength() -> str`
**Purpose**: Get strength in standardized format for exact comparison.

**Logic**:
1. Convert to uppercase
2. Remove commas: `"1,000MG"` → `"1000MG"`
3. Remove brackets/quotes: `"['10MG']"` → `"10MG"`
4. Remove whitespace: `"10 MG"` → `"10MG"`
5. Normalize "MG." to "MG"
6. Preserve MCG, ML units

**Example**: `"10 mg"` → `"10MG"`

#### `get_dosage_form_tokens() -> List[str]`
**Purpose**: Get dosage form as unique token list for overlap matching.

**Logic**:
1. Get dosage form string
2. Call `_normalize_tokens()`
3. Return list of unique tokens

**Example**: `"TABLET, EXTENDED RELEASE"` → `["TABLET", "EXTENDED", "RELEASE"]`

#### `get_route_tokens() -> List[str]`
**Purpose**: Get route as unique token list for overlap matching.

**Example**: `"ORAL;SUBLINGUAL"` → `["ORAL", "SUBLINGUAL"]`

### Private Method: `_normalize_tokens(value: str) -> List[str]`
**Purpose**: Internal tokenization logic (same as `norm_tokens()` in match.py).

**Logic**:
1. Convert to uppercase
2. Remove brackets/quotes
3. Remove non-alphanumeric characters
4. Collapse whitespace with `str_squish()`
5. Split on spaces
6. Remove duplicates while preserving order
7. Return ordered unique token list

### String Representation
```python
def __repr__(self) -> str:
    return f"ANDA({self.anda_number}: {self.get_ingredient()} {self.get_strength()})"
```

**Example**: `ANDA(074830: METFORMIN HYDROCHLORIDE 500MG)`

---

## 2. NDA Class

### Purpose
Encapsulates all NDA (brand-name drug) data from main table and Orange Book.

### Initialization
```python
def __init__(self, main_table_row: pd.Series, orange_book_rows: pd.DataFrame = None):
    self._main_data = main_table_row.copy()
    self._ob_data = orange_book_rows if orange_book_rows is not None else pd.DataFrame()
    self.nda_number = str(self._main_data.get('Appl_No', ''))
    self.name = self.nda_number
```

**Logic**:
1. Stores main table row as `_main_data` (primary source)
2. Stores Orange Book rows as `_ob_data` (supplementary)
3. Extracts NDA number as primary identifier
4. Uses NDA number as object name

**Why Two Data Sources?**
- Main table: Has NDA-level aggregated data (MMT, approval dates)
- Orange Book: Has product-level details (strengths, trade names, companies)

### Core Getter Methods

#### From Main Table
- **`get_nda_number() -> str`**: NDA application number
- **`get_applicant() -> str`**: Primary company name from main table
- **`get_approval_date() -> Optional[datetime]`**: Approval date as datetime
- **`get_ingredient() -> str`**: Active ingredient
- **`get_strength_list() -> str`**: List of strengths (may be multi-strength)
- **`get_dosage_form() -> str`**: Dosage form
- **`get_route() -> str`**: Route of administration
- **`get_product_count() -> int`**: Number of products
- **`get_strength_count() -> int`**: Number of strengths
- **`get_mmt() -> str`**: Market Monopoly Time designation
- **`get_mmt_years() -> float`**: Granted monopoly period in years

#### From Orange Book
- **`get_orange_book_products() -> pd.DataFrame`**: All OB product rows for this NDA
- **`get_companies_from_orange_book() -> List[str]`**: All company names from OB
- **`get_strengths_from_orange_book() -> List[str]`**: All strengths from OB
- **`get_trade_names() -> List[str]`**: All trade names from OB

#### Combined Methods
**`get_all_companies() -> List[str]`**
**Purpose**: Get comprehensive list of all companies from both sources.

**Logic**:
1. Start with main table applicant
2. Add Orange Book companies
3. Remove duplicates
4. Return combined list

**Why Important**: Company validation needs all possible company names (parent companies, subsidiaries, name changes).

### Normalized Methods for Matching
Same as ANDA class:
- `get_normalized_ingredient() -> str`
- `get_dosage_form_tokens() -> List[str]`
- `get_route_tokens() -> List[str]`
- `_normalize_tokens(value: str) -> List[str]`

### String Representation
```python
def __repr__(self) -> str:
    return f"NDA({self.nda_number}: {self.get_ingredient()} - {self.get_applicant()})"
```

**Example**: `NDA(021513: ATORVASTATIN CALCIUM - PFIZER INC)`

---

## 3. Match Class

### Purpose
Represents a single NDA with its list of matching ANDAs, providing methods for validation and analysis.

### Initialization
```python
def __init__(self, nda: NDA, initial_andas: List[ANDA] = None):
    self.nda = nda
    self._andas = initial_andas if initial_andas else []
    self.name = f"NDA{nda.get_nda_number()}_match"
    
    # Automatically eliminate impossible matches
    self._andas = self.eliminate_impossible_matches(self._andas)
```

**Logic**:
1. Store NDA object reference
2. Initialize ANDA list (empty or provided)
3. Create descriptive name
4. **Immediately validate**: Remove ANDAs approved before NDA

### ANDA Management Methods

#### `add_anda(anda: ANDA) -> None`
**Purpose**: Add an ANDA to this match after validation.

**Logic**:
1. Call `eliminate_impossible_matches([anda])`
2. If valid (not eliminated), add to `_andas` list

#### `remove_anda(anda_number: str) -> None`
**Purpose**: Remove an ANDA by number.

**Logic**: Filter `_andas` list to exclude matching ANDA number

#### `get_matches() -> List[ANDA]`
**Purpose**: Get copy of all matched ANDAs.

**Returns**: New list (prevents external modification of internal state)

#### `get_match_count() -> int`
**Purpose**: Get number of matched ANDAs.

#### `get_match_numbers_in_date_order() -> List[str]`
**Purpose**: Get ANDA numbers sorted by approval date (earliest first).

**Logic**:
1. Separate ANDAs with dates from ANDAs without dates
2. Sort ANDAs with dates chronologically
3. Return dated ANDAs first, then undated ones

**Usage**: Identifying first ANDA competitor for monopoly time calculation.

### Validation Methods

#### `eliminate_impossible_matches(andas: List[ANDA]) -> List[ANDA]`
**Purpose**: Remove ANDAs with approval dates before NDA approval (chronologically impossible).

**Logic**:
1. Get NDA approval date
2. If no NDA date, keep all ANDAs (can't validate)
3. For each ANDA:
   - Get ANDA approval date
   - If no ANDA date, keep it (can't validate)
   - If ANDA date < NDA date: **Eliminate** (impossible)
   - If ANDA date ≥ NDA date: Keep (valid)
4. Log elimination count and details
5. Return valid ANDAs only

**Example**:
```
NDA approved: 2000-01-15
ANDA approved: 1999-12-01 → ELIMINATED (before NDA)
ANDA approved: 2005-03-20 → KEPT (after NDA)
ANDA approved: None       → KEPT (can't validate)
```

#### `verify_matches(orange_book_clean, validation_function, use_pdf_validation) -> Match`
**Purpose**: Main validation entry point with multiple strategies.

**Parameters**:
- `orange_book_clean`: Orange Book DataFrame for company data
- `validation_function`: Optional custom validation function
- `use_pdf_validation`: Whether to use full PDF-based validation (default True)

**Logic**:
1. If custom validation function provided → use it
2. Else if `use_pdf_validation=True` → call `_pdf_based_validation()`
3. Else → call `_conservative_validation()`
4. Return self (for method chaining)

**Method Chaining Example**:
```python
match.verify_matches(orange_book, use_pdf_validation=True).calculate_monopoly_time()
```

### PDF-Based Validation (Full Pipeline)

#### `_pdf_based_validation(orange_book_clean: pd.DataFrame) -> List[ANDA]`
**Purpose**: Comprehensive validation using FDA approval letter PDFs.

**Logic** (mirrors postprocess.py logic):

1. **Create temporary DataFrame** for validation:
   ```python
   match_rows = [{
       'NDA_Appl_No': self.nda.get_nda_number(),
       'ANDA_Appl_No': anda.get_anda_number(),
       'ANDA_Approval_Date_Date': anda.get_approval_date()
   } for anda in self._andas]
   ```

2. **Get NDA companies**:
   ```python
   nda_companies = {self.nda.get_nda_number(): self.nda.get_all_companies()}
   ```
   - If no companies found, keep all matches (can't validate)

3. **Extract ANDA PDF URLs**:
   ```python
   anda_pdf_urls = extract_anda_pdf_urls(anda_matches_df, test_urls=True)
   ```
   - Uses `extract_from_pdf.py` functions
   - Tests URL patterns to find working links

4. **Extract company references from PDFs**:
   ```python
   company_references = extract_company_references_from_pdfs(anda_pdf_urls)
   ```
   - Parses bioequivalence statements
   - Extracts company names

5. **Validate matches** (90% similarity threshold):
   ```python
   validated_matches, rejected_matches = validate_company_matches(
       nda_companies, company_references, anda_matches_df, similarity_threshold=0.9
   )
   ```
   - Uses `postprocess.py` validation logic
   - 90% similarity = strong company match required

6. **Filter ANDAs** to keep only validated ones:
   ```python
   validated_anda_numbers = set(validated_matches['ANDA_Appl_No'])
   validated_andas = [anda for anda in self._andas 
                      if anda.get_anda_number() in validated_anda_numbers]
   ```

7. **Fallback behavior** if validation fails:
   - If PDF extraction fails → keep all matches (conservative)
   - If error occurs → call `_conservative_validation()`

**Logging**:
```
INFO - Starting PDF-based validation for NDA 021513 with 45 ANDAs
INFO - Extracting PDF URLs for 45 ANDAs...
INFO - Found 23 working PDF URLs
INFO - Extracting company references from PDFs...
INFO - Extracted references from 18 PDFs
INFO - Validating matches using company references...
✓ Validated: NDA 021513 - ANDA 074830 (Company: PFIZER INC, Similarity: 1.00)
✗ Rejected: NDA 021513 - ANDA 076703 (Low company similarity: 0.15)
INFO - PDF validation kept 40/45 matches for NDA 021513
```

### Conservative Validation (Fallback)

#### `_conservative_validation(orange_book_clean: pd.DataFrame) -> List[ANDA]`
**Purpose**: Minimal validation when PDF data unavailable.

**Logic**:
1. Get NDA companies
2. If no companies, keep all matches
3. Return all matches (no elimination)

**Conservative Approach**:
- Only rejects matches with **positive evidence** of mismatch
- Missing data ≠ invalid match
- Prevents false negatives from data gaps

### API-Based Validation (Placeholder)

#### `validate_company_matches_api(api_key: str) -> Match`
**Purpose**: Future implementation using FDA API directly.

**Current Status**: Not implemented, logs warning

**Planned Logic**:
1. Call `get_api_submissions()` with ANDAs
2. Extract company references from PDFs
3. Validate using company matching
4. Update `_andas` list

### Analysis Methods

#### `calculate_monopoly_time() -> Optional[float]`
**Purpose**: Calculate monopoly period from NDA approval to first ANDA approval.

**Logic**:
1. Get NDA approval date (return None if missing)
2. Find earliest ANDA approval date:
   ```python
   for anda in self._andas:
       anda_approval = anda.get_approval_date()
       if anda_approval and (earliest is None or anda_approval < earliest):
           earliest = anda_approval
   ```
3. Calculate days: `(earliest_anda - nda_approval).days`
4. Convert to years: `monopoly_years = monopoly_days / 365.25`
5. Log result and return

**Example**:
```
NDA approved: 2000-01-15
Earliest ANDA: 2012-03-20 (ANDA 074830)
Monopoly days: 4,448
Monopoly years: 12.18
```

#### `get_monopoly_summary() -> Dict[str, Any]`
**Purpose**: Get comprehensive monopoly time analysis.

**Returns**:
```python
{
    'nda_number': '021513',
    'nda_approval_date': '2000-01-15',
    'nda_company': 'PFIZER INC',
    'ingredient': 'ATORVASTATIN CALCIUM',
    'granted_monopoly_years': 3.0,
    'actual_monopoly_years': 12.18,
    'shorter_than_granted': False,
    'matching_anda_count': 45,
    'matching_anda_numbers': ['074830', '076703', ...]
}
```

#### `get_validation_summary() -> Dict[str, Any]`
**Purpose**: Get validation status details.

**Returns**:
```python
{
    'nda_number': '021513',
    'nda_companies': ['PFIZER INC', 'PARKE-DAVIS DIV'],
    'total_andas': 45,
    'anda_numbers': ['074830', '076703', ...],
    'validation_method': 'PDF-based'
}
```

### String Representation
```python
def __repr__(self) -> str:
    return f"Match({self.name}: {len(self._andas)} ANDAs)"
```

**Example**: `Match(NDA021513_match: 45 ANDAs)`

---

## Integration with postprocess.py

The Match class uses these postprocess.py functions:
1. **`extract_anda_pdf_urls()`**: Get PDF URLs for ANDA approval letters
2. **`extract_company_references_from_pdfs()`**: Parse PDFs to extract company names
3. **`calculate_text_similarity()`**: Calculate company name similarity scores
4. **`validate_company_matches()`**: Core validation logic with 90% threshold

This creates a tight integration between OOP matching and functional validation.

---

## Complete Usage Example

```python
from match_class import NDA, ANDA, Match
import pandas as pd

# Load data
main_table = pd.read_excel("main_table.xlsx")
orange_book = pd.read_excel("orange_book.xlsx")

# Create NDA object
nda_row = main_table[main_table['Appl_No'] == '021513'].iloc[0]
nda_ob_rows = orange_book[(orange_book['Appl_No'] == '021513') & (orange_book['Appl_Type'] == 'N')]
nda = NDA(nda_row, nda_ob_rows)

# Create ANDA objects (for same ingredient)
anda_rows = orange_book[
    (orange_book['Appl_Type'] == 'A') & 
    (orange_book['Ingredient'] == nda.get_ingredient())
]
andas = [ANDA(row) for _, row in anda_rows.iterrows()]

# Create match and add ANDAs
match = Match(nda)  # Automatically eliminates impossible matches
for anda in andas:
    match.add_anda(anda)  # Validates each ANDA

print(f"Initial matches: {match.get_match_count()}")

# Verify matches using PDF validation
match.verify_matches(orange_book, use_pdf_validation=True)
print(f"After PDF validation: {match.get_match_count()}")

# Calculate monopoly time
monopoly_years = match.calculate_monopoly_time()
print(f"Monopoly time: {monopoly_years:.2f} years")

# Get full summary
summary = match.get_monopoly_summary()
print(f"Granted: {summary['granted_monopoly_years']:.1f} years")
print(f"Actual: {summary['actual_monopoly_years']:.1f} years")
print(f"Shorter than granted: {summary['shorter_than_granted']}")

# Get ANDAs in date order
anda_numbers = match.get_match_numbers_in_date_order()
print(f"First ANDA: {anda_numbers[0]}")
```

## Why Not Used in Production?

### Advantages of match.py (Functional)
1. **Performance**: Pandas vectorization is faster than object iteration
2. **Batch operations**: Process thousands of matches simultaneously
3. **Memory efficiency**: No object overhead
4. **Simpler debugging**: DataFrame inspection is straightforward

### Advantages of match_class.py (OOP)
1. **Encapsulation**: Each NDA/ANDA is self-contained
2. **Method chaining**: `match.verify_matches().calculate_monopoly_time()`
3. **Validation logic**: Built into objects
4. **Testability**: Individual object unit tests

### Conclusion
- **match_class.py**: Better for **understanding** and **testing** the logic
- **match.py**: Better for **production** performance and scalability

## Dependencies

### Required Libraries
- **pandas**: Data manipulation
- **numpy**: NaN handling
- **re**: Regex for normalization
- **datetime**: Date handling
- **typing**: Type hints
- **logging**: Progress tracking

### Internal Dependencies
- **preprocess.str_squish**: Whitespace normalization
- **postprocess.extract_anda_pdf_urls**: PDF URL extraction
- **postprocess.extract_company_references_from_pdfs**: PDF parsing
- **postprocess.calculate_text_similarity**: Company matching
- **postprocess.validate_company_matches**: Validation logic

## Future Enhancements

1. **Complete API Validation**: Implement `validate_company_matches_api()`
2. **Caching**: Store validation results to avoid re-validation
3. **Batch Operations**: Add class methods for bulk NDA/ANDA creation
4. **Custom Matching Criteria**: Allow configurable matching thresholds
5. **Export to DataFrame**: Convert Match objects back to DataFrames for analysis
