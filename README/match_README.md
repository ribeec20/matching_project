# match.py - Functional NDA-ANDA Matching Algorithm

## Overview
This module implements the **functional/procedural approach** to matching NDA products with ANDA products using a **3-criteria matching algorithm**. This is the production version used in `dosage.py` and `dosage_2025.py`.

## Purpose
**Product-Level Matching**: Identifies which ANDA products are bioequivalent to which NDA products based on ingredient, dosage form, route, and strength. The algorithm operates at the product level, finding all NDA-ANDA product pairs that meet the matching criteria.

## Key Differences: match.py vs match_class.py
- **match.py** (this file): Functional/DataFrame-based matching algorithm used in production pipelines
- **match_class.py**: Defines NDA, ANDA, and Match classes as object-oriented data containers with validation methods

## Core Matching Algorithm: The 3 Criteria

### Required Matches (All 3 must be TRUE)
1. **DF_OK** (Dosage Form): Dosage forms must have overlapping tokens
2. **RT_OK** (Route): Routes of administration must have overlapping tokens  
3. **STR_OK** (Strength): Strengths must match exactly after format normalization

### Historical Note: TE_OK Removed
Early versions included **TE_OK** (Therapeutic Equivalence), but this was removed because:
- TE codes change over time
- Many valid bioequivalent products lack TE codes
- Creates false negatives in matching

## Data Structures

### MatchData Dataclass
Container for all DataFrames produced during matching:

```python
@dataclass
class MatchData:
    study_ndas: pd.DataFrame              # NDAs from main table merged with Orange Book
    study_ndas_strength: pd.DataFrame     # NDAs with strength processing
    ndas_ob: pd.DataFrame                 # All NDA records from Orange Book
    andas_ob: pd.DataFrame                # All ANDA records from Orange Book (prefixed)
    study_ndas_final: pd.DataFrame        # Final cleaned NDA dataset (NDA_ prefixed)
    candidates: pd.DataFrame              # All ingredient-matched NDA-ANDA pairs
    anda_matches: pd.DataFrame            # Final matches passing all 3 criteria
    nda_summary: pd.DataFrame             # NDA-level summary for monopoly analysis
    ob_nda_first: pd.DataFrame            # Earliest approval dates from Orange Book
    date_check: pd.DataFrame              # Date validation between sources
```

## Normalization Functions

### 1. `norm_strength(value: object) -> str | float`
**Purpose**: Normalize strength values for exact comparison.

**Logic**:
1. Handle NaN/None → return np.nan
2. Convert to uppercase string
3. Remove commas: `"1,000MG"` → `"1000MG"`
4. Remove brackets/quotes: `"['10MG']"` → `"10MG"`
5. Remove whitespace: `"10 MG"` → `"10MG"`
6. Normalize units: `"10MG."` → `"10MG"`
7. Preserve MCG, ML units

**Examples**:
```
"10 mg"        → "10MG"
"1,000 MG"     → "1000MG"
"['500MG']"    → "500MG"
"0.5MCG"       → "0.5MCG"
```

### 2. `tokenize_strength_list(value: object) -> List[str]`
**Purpose**: Handle multi-strength products (e.g., "10MG | 20MG | 40MG").

**Logic**:
1. Remove brackets/quotes from list representation
2. Split on delimiters: `|`, `;`, `,`
3. Normalize each strength token individually
4. Return list of normalized strengths

**Example**:
```python
"['10MG', '20MG', '40MG']" → ["10MG", "20MG", "40MG"]
"10mg | 20mg | 40mg"       → ["10MG", "20MG", "40MG"]
```

### 3. `norm_tokens(value: object) -> List[str]`
**Purpose**: Normalize dosage forms and routes into unique token lists.

**Logic**:
1. Convert to uppercase
2. Remove brackets/quotes
3. Remove all non-alphanumeric characters
4. Apply `str_squish()` to collapse whitespace
5. Split into tokens
6. Remove duplicates while preserving order
7. Return ordered list of unique tokens

**Examples**:
```
"TABLET, EXTENDED RELEASE" → ["TABLET", "EXTENDED", "RELEASE"]
"['CAPSULE']"               → ["CAPSULE"]
"ORAL;SUBLINGUAL"           → ["ORAL", "SUBLINGUAL"]
```

### 4. `has_overlap(left: List[str], right: List[str]) -> bool`
**Purpose**: Check if two token lists share any common elements.

**Logic**:
1. If either list is empty → False
2. Convert both to sets
3. Check intersection
4. Return True if any overlap exists

**Examples**:
```python
["TABLET", "ORAL"] ∩ ["TABLET", "SUBLINGUAL"] → True (TABLET overlaps)
["CAPSULE"]        ∩ ["TABLET"]               → False (no overlap)
["ORAL"]           ∩ []                       → False (empty list)
```

## Main Matching Pipeline: `match_ndas_to_andas()`

### Step 1: Extract and Prepare Base Datasets
```python
ndas_ob, andas_ob = _extract_nda_and_anda_data(orange_book_clean)
```

**Logic** (`_extract_nda_and_anda_data`):
1. Filter Orange Book for NDAs (Appl_Type == "N")
2. Drop redundant Appl_Type column from NDAs
3. Filter Orange Book for ANDAs (Appl_Type == "A")
4. Prefix all ANDA columns with "ANDA_" to avoid naming conflicts
5. Drop ANDA_Appl_Type column (redundant after filtering)

**Why Prefix ANDAs?**
- Prevents column name collisions during merging
- Makes it clear which data comes from NDAs vs ANDAs
- Example: `Appl_No` vs `ANDA_Appl_No`

### Step 2: Merge Study NDAs with Orange Book
```python
study_ndas = _merge_study_ndas_with_orange_book(main_table_clean, ndas_ob)
```

**Logic**:
1. Left join main_table_clean → ndas_ob on Appl_No
2. Preserves all NDAs from study (main table)
3. Adds Orange Book product details
4. Creates `_nda` suffix for overlapping columns

**Result**: Each study NDA row gets matched with its Orange Book products

### Step 3: Process Strength Matching
```python
study_ndas_strength = _process_strength_matching(study_ndas)
```

**Logic** (`_process_strength_matching`):
1. Store raw strengths: `strength_x_raw`, `strength_y_raw`
2. **Tokenize main table strength** (can be multi-strength list):
   ```python
   strength_x_tokens = tokenize_strength_list(strength_x_raw)
   ```
3. **Normalize Orange Book strength** (single value):
   ```python
   strength_y_norm = norm_strength(strength_y_raw)
   ```
4. **Check if OB strength in main table tokens**:
   ```python
   strength_y_in_tokens = strength_in_tokens(strength_x_tokens, strength_y_norm)
   ```
5. **Also check substring containment** (alternative matching):
   ```python
   strength_x_norm = norm_strength(strength_x_raw)
   strength_y_in_substr = substr_contains(strength_x_norm, strength_y_norm)
   ```
6. **Final strength match** (either method works):
   ```python
   strength_match = strength_y_in_tokens | strength_y_in_substr
   ```

**Why Two Methods?**
- Tokenization: Handles explicit lists like `["10MG", "20MG"]`
- Substring: Handles implicit lists like `"10MG20MG40MG"`

### Step 4: Process Date Validation
```python
ob_nda_first, date_check = _process_date_validation(study_ndas, ndas_ob)
```

**Logic** (`_process_date_validation`):
1. Convert Orange Book approval dates to datetime
2. Group by NDA number, find earliest approval date per NDA
3. Create `ob_nda_first` DataFrame with earliest dates
4. Merge with main table dates for comparison
5. Calculate `date_equal` and `date_diff_days` for validation

**Purpose**: Validates that main table and Orange Book agree on NDA approval dates

### Step 5: Consolidate Study NDA Data
```python
study_ndas_final = _consolidate_study_nda_data(study_ndas_strength)
```

**Logic** (`_consolidate_study_nda_data`):
1. **Coalesce data** from main table and Orange Book (main table priority):
   ```python
   Ingredient = coalesce_str(Ingredient, Ingredient_nda)
   Approval_Date = coalesce_str(Approval_Date, Approval_Date_nda)
   DF = coalesce_str(DF, DF_nda)
   Route = coalesce_str(Route, Route_nda)
   ```
2. **Rename strength columns** for clarity:
   ```python
   Strength → Strength_List (from main table, can be multi-strength)
   Strength_nda → Strength_Specific (from OB, single strength)
   ```
3. **Reorganize columns**:
   - Front: Essential matching fields (Appl_No, Ingredient, Dates, DF, Route, Strengths)
   - Optional: MMT fields (only if present in main table)
   - Back: All other columns
4. **Drop temporary columns**: `_x`, `_y`, `_nda` suffixes, strength processing columns
5. **Prefix all columns with NDA_**: Prevents conflicts when merging with ANDAs

**Column Structure After Consolidation**:
```
NDA_Appl_No
NDA_Ingredient
NDA_Approval_Date
NDA_DF
NDA_Route
NDA_Strength_List
NDA_Strength_Specific
NDA_Product_No
[NDA_MMT_Years]  (if present)
... other NDA columns ...
```

### Step 6: Prepare Matching Datasets
```python
nda_prod, andas_prep = _prepare_matching_datasets(study_ndas_final, andas_ob)
```

**Logic** (`_prepare_matching_datasets`):

**For NDAs**:
1. Create normalized ingredient key:
   ```python
   NDA_ING_KEY = str_squish(NDA_Ingredient).upper()
   ```
2. Tokenize dosage form:
   ```python
   NDA_DF_TOK = norm_tokens(NDA_DF)
   ```
3. Tokenize route:
   ```python
   NDA_RT_TOK = norm_tokens(NDA_Route)
   ```
4. Normalize strength:
   ```python
   NDA_STR_N = norm_strength(NDA_Strength_Specific)
   ```

**For ANDAs** (same process, ANDA_ prefix):
1. `ANDA_ING_KEY` = normalized ingredient
2. `ANDA_DF_TOK` = dosage form tokens
3. `ANDA_RT_TOK` = route tokens
4. `ANDA_STR_N` = normalized strength
5. `ANDA_Approval_Date_Date` = datetime conversion

**Result**: Two DataFrames ready for matching with normalized fields

### Step 7: Ingredient-Based Matching
```python
candidates = _perform_ingredient_based_matching(nda_prod, andas_prep)
```

**Logic**:
1. Inner join on ingredient:
   ```python
   merge(left_on="NDA_ING_KEY", right_on="ANDA_ING_KEY", how="inner")
   ```
2. Creates Cartesian product: **All NDA-ANDA pairs with same ingredient**
3. Candidate count = (NDA products with ingredient) × (ANDA products with ingredient)

**Example**:
- Ingredient: ATORVASTATIN CALCIUM
- NDA products: 4 different strengths
- ANDA products: 100 different products
- Candidates: 4 × 100 = 400 potential matches

### Step 8: Apply the 3 Matching Criteria
```python
candidates = _apply_matching_criteria(candidates)
```

**Logic** (`_apply_matching_criteria`):

**Criterion 1: DF_OK (Dosage Form Overlap)**
```python
candidates["DF_OK"] = candidates.apply(
    lambda row: has_overlap(row["NDA_DF_TOK"], row["ANDA_DF_TOK"]),
    axis=1
)
```
- Checks if dosage form tokens overlap
- Example: `["TABLET"]` overlaps with `["TABLET", "EXTENDED", "RELEASE"]` → True

**Criterion 2: RT_OK (Route Overlap)**
```python
candidates["RT_OK"] = candidates.apply(
    lambda row: has_overlap(row["NDA_RT_TOK"], row["ANDA_RT_TOK"]),
    axis=1
)
```
- Checks if route tokens overlap
- Example: `["ORAL"]` overlaps with `["ORAL"]` → True

**Criterion 3: STR_OK (Strength Exact Match)**
```python
candidates["STR_OK"] = candidates["NDA_STR_N"] == candidates["ANDA_STR_N"]
```
- Checks exact equality after normalization
- Example: `"10MG"` == `"10MG"` → True
- Example: `"10MG"` == `"20MG"` → False

### Step 9: Filter Final Matches
```python
anda_matches = _filter_final_matches(candidates)
```

**Logic**:
```python
candidates.query("DF_OK and RT_OK and STR_OK").copy()
```
- Keeps only rows where **ALL 3 criteria are TRUE**
- Removes all other candidate pairs

**Filtering Impact Example**:
```
Candidates: 10,000 pairs
DF_OK fails: -3,000 pairs (wrong dosage form)
RT_OK fails: -1,500 pairs (wrong route)
STR_OK fails: -4,000 pairs (wrong strength)
Final matches: 1,500 pairs
```

### Step 10: Create NDA Summary
```python
nda_summary = _create_nda_summary(study_ndas_final)
```

**Logic**:
1. Extract essential NDA-level columns:
   ```python
   ["NDA_Appl_No", "NDA_Approval_Date", "NDA_Ingredient", "NDA_Applicant"]
   ```
2. Add `NDA_MMT_Years` if present (2025 data may not have this)
3. Drop duplicates by NDA_Appl_No (collapse to one row per NDA)
4. Convert approval date to datetime: `NDA_Approval_Date_Date`

**Purpose**: Creates NDA-level view for downstream monopoly time calculations

### Step 11: Return MatchData
```python
return MatchData(
    study_ndas=study_ndas,
    study_ndas_strength=study_ndas_strength,
    ndas_ob=ndas_ob,
    andas_ob=andas_ob,
    study_ndas_final=study_ndas_final,
    candidates=candidates,
    anda_matches=anda_matches,
    nda_summary=nda_summary,
    ob_nda_first=ob_nda_first,
    date_check=date_check,
)
```

**What's Included**:
- **Raw data**: study_ndas, ndas_ob, andas_ob
- **Processed data**: study_ndas_strength, study_ndas_final
- **Matching results**: candidates (all), anda_matches (filtered)
- **Validation data**: ob_nda_first, date_check
- **Summary data**: nda_summary

## Helper Functions

### `coalesce_str(value_a, value_b) -> object`
**Purpose**: Return first non-null, non-empty value.

**Logic**:
1. Handle NaN floats
2. Convert pd.Timestamp to string format
3. Return `value_a` if not null/empty, else `value_b`

### `shorter_than_granted(row: pd.Series) -> bool`
**Purpose**: Check if actual monopoly is shorter than granted period.

**Logic**:
1. Extract `Actual_Monopoly_Years_Prod` and `NDA_MMT_Years`
2. If either is NaN → return np.nan
3. Return `actual < granted`

### `te_compatible(val_a, val_b) -> bool`
**Purpose**: Check TE code compatibility (legacy function, not used in current matching).

**Logic**: Returns True if either value is None or if they're equal

### `substr_contains(text, pattern) -> bool`
**Purpose**: Simple substring matching with NaN handling.

## Column Naming Conventions

### Prefixes
- **NDA_**: Columns from NDA data
- **ANDA_**: Columns from ANDA data
- **No prefix**: Temporary/intermediate columns

### Suffixes
- **_raw**: Original value before normalization
- **_norm**: Normalized value
- **_tokens**: List of normalized tokens
- **_x**: From first DataFrame in merge
- **_y**: From second DataFrame in merge
- **_nda**: From Orange Book NDA data
- **_Date**: Datetime parsed version

### Special Column Names
- **ING_KEY**: Normalized ingredient for matching
- **DF_TOK**: Dosage form tokens
- **RT_TOK**: Route tokens
- **STR_N**: Normalized strength
- **DF_OK**: Dosage form match flag
- **RT_OK**: Route match flag
- **STR_OK**: Strength match flag

## Matching Algorithm Flowchart

```
Orange Book (49,983 products)
    ↓ Split by Appl_Type
    ├─→ NDAs (Appl_Type = "N")
    └─→ ANDAs (Appl_Type = "A") → Prefix with "ANDA_"
    
Main Table (619 NDAs)
    ↓ Left join with NDAs on Appl_No
Study NDAs (1,157 NDA products)
    ↓ Process strengths (tokenize, normalize)
Study NDAs Strength
    ↓ Consolidate, coalesce, clean
Study NDAs Final (NDA_ prefixed)
    ↓ Prepare: normalize ingredient, tokenize DF/Route, normalize strength
NDA Products Ready (with NDA_ING_KEY, NDA_DF_TOK, NDA_RT_TOK, NDA_STR_N)
    
ANDAs (ANDA_ prefixed)
    ↓ Prepare: same normalization as NDAs
ANDA Products Ready (with ANDA_ING_KEY, ANDA_DF_TOK, ANDA_RT_TOK, ANDA_STR_N)
    
    ↓ Inner join on ingredient
Candidates (10,000+ NDA-ANDA pairs)
    ↓ Apply 3 criteria
    ├─→ DF_OK = has_overlap(NDA_DF_TOK, ANDA_DF_TOK)
    ├─→ RT_OK = has_overlap(NDA_RT_TOK, ANDA_RT_TOK)
    └─→ STR_OK = (NDA_STR_N == ANDA_STR_N)
    
    ↓ Filter: DF_OK AND RT_OK AND STR_OK
Final Matches (1,500+ valid NDA-ANDA pairs)
```

## Performance Characteristics

### Computational Complexity
- **Ingredient matching**: O(n × m) where n = NDA products, m = ANDA products
- **Token overlap**: O(k) where k = average token count (~3-5)
- **Strength comparison**: O(1) exact match
- **Overall**: ~10-30 seconds for full Orange Book dataset

### Memory Usage
- Candidates DataFrame: ~100-500 MB (10,000+ rows × 100+ columns)
- Final matches: ~10-50 MB (1,500+ rows)
- Peak memory: ~1-2 GB

### Scalability
- **Current**: 619 NDAs, 49,983 products, 10,000+ candidates → 1,500+ matches
- **2025 Data**: Similar performance with updated Orange Book
- **Bottleneck**: Ingredient-based Cartesian product (candidates creation)

## Common Edge Cases

### 1. Multi-Strength Products
**Problem**: Main table lists "10MG | 20MG | 40MG", Orange Book has separate products for each.

**Solution**: `tokenize_strength_list()` splits list, `strength_in_tokens()` checks membership.

### 2. Missing MMT Data (2025 Dataset)
**Problem**: 2025 Orange Book data doesn't include MMT_Years.

**Solution**: Optional columns in `_consolidate_study_nda_data()`, graceful degradation.

### 3. Date Mismatches
**Problem**: Main table and Orange Book have different approval dates for same NDA.

**Solution**: Coalesce with priority to main table, track differences in `date_check`.

### 4. Empty Token Lists
**Problem**: Missing or null dosage form/route data.

**Solution**: `has_overlap()` returns False for empty lists (no match).

## Integration Points

### Input (from preprocess.py)
```python
main_table_clean, orange_book_clean = preprocess_data(main_path, orange_path)
```

### Output (to postprocess.py)
```python
match_data = match_ndas_to_andas(main_table_clean, orange_book_clean)
validated_matches, rejected_matches, details = nda_anda_company_validation(
    match_data, orange_book_clean, main_table_clean
)
```

### Visualization (to monopoly_time.py)
```python
outputs = build_postprocess_outputs(match_data)
plot_monopoly_scatter(outputs["nda_monopoly_times"])
```

## Dependencies

### Required Libraries
- **pandas**: DataFrame operations, merging, grouping
- **numpy**: NaN handling, numerical operations
- **re**: Regular expression pattern matching for normalization
- **dataclasses**: MatchData container
- **typing**: Type hints

### Internal Dependencies
- **preprocess.str_squish**: Whitespace normalization

## Usage Example

```python
from preprocess import preprocess_data
from match import match_ndas_to_andas

# Load and clean data
main_clean, ob_clean = preprocess_data("main_table.xlsx", "orange_book.xlsx")

# Run matching algorithm
match_data = match_ndas_to_andas(main_clean, ob_clean)

# Inspect results
print(f"Total candidates: {len(match_data.candidates)}")
print(f"Final matches: {len(match_data.anda_matches)}")
print(f"Match rate: {len(match_data.anda_matches) / len(match_data.candidates) * 100:.1f}%")

# Check matching criteria breakdown
print(f"DF_OK: {match_data.candidates['DF_OK'].sum()}")
print(f"RT_OK: {match_data.candidates['RT_OK'].sum()}")
print(f"STR_OK: {match_data.candidates['STR_OK'].sum()}")
print(f"All 3: {len(match_data.anda_matches)}")
```

## Future Enhancements

1. **Fuzzy Strength Matching**: Allow small variations (e.g., 9.9MG ≈ 10MG)
2. **Partial Token Matching**: Accept 2 of 3 tokens matching instead of any overlap
3. **Performance Optimization**: Use pandas vectorization instead of apply()
4. **Caching**: Store normalized tokens to avoid recomputation
5. **Parallel Processing**: Split ingredient groups across multiple cores
