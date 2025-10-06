# NDA-ANDA Matching Project

## Overview
This project analyzes pharmaceutical NDA (New Drug Application) and ANDA (Abbreviated New Drug Application) data to calculate **monopoly times** - the period between NDA approval and the first generic ANDA approval. The analysis validates matches using FDA approval letter PDFs and company name verification.

## Key Features
- **3-Criteria Matching Algorithm**: Match generic drugs (ANDAs) to reference branded drugs (NDAs) based on ingredient, dosage form, route, and strength
- **FDA API Integration**: Query FDA API for ANDA approval letter PDFs
- **PDF Company Validation**: Extract and verify company references from approval letters to validate matches
- **Monopoly Time Calculation**: Calculate actual market exclusivity and compare to granted monopoly periods
- **Interactive Visualization**: Generate Plotly scatter plots with detailed hover information

## Pipeline Architecture

### Module Flow
```
┌─────────────────────────────────────────────────────────────────────┐
│                         dosage.py (Main Pipeline)                    │
└─────────────────────────────────────────────────────────────────────┘
                                    │
        ┌───────────────────────────┼───────────────────────────┐
        ▼                           ▼                           ▼
┌──────────────┐          ┌──────────────┐          ┌──────────────┐
│ preprocess.py│          │   match.py   │          │postprocess.py│
│              │          │              │          │              │
│ • Load Excel │──────────▶• 3 Criteria  │──────────▶• FDA API     │
│ • Clean data │          │  Matching    │          │• PDF Extract │
│ • Normalize  │          │• DF_OK       │          │• Validation  │
│ • Parse dates│          │• RT_OK       │          │• Monopoly    │
└──────────────┘          │• STR_OK      │          │  Times       │
                          └──────────────┘          └──────┬───────┘
                                                           │
                          ┌──────────────┐          ┌─────▼────────┐
                          │drugs_api.py  │◀─────────│              │
                          │              │          │              │
                          │ • FDA API    │          │              │
                          │ • Rate Limit │          │              │
                          └──────────────┘          │              │
                                                    │              │
                          ┌──────────────┐          │              │
                          │extract_from_ │◀─────────┤              │
                          │  pdf.py      │          │              │
                          │              │          │              │
                          │ • PDF Parse  │          │              │
                          │ • Company    │          │              │
                          │   Extract    │          └──────────────┘
                          └──────────────┘                 │
                                                           │
                                                    ┌──────▼───────┐
                                                    │monopoly_time.│
                                                    │     py       │
                                                    │              │
                                                    │• Plotly Plot │
                                                    │• Interactive │
                                                    └──────────────┘
```

### Data Flow (Step-by-Step)

**Step 1: Data Loading & Preprocessing** (`preprocess.py`)
```
Excel Files → Load → Clean → Normalize
├─ Main Table (619 NDAs)
│  └─ Clean: Squish text, parse dates, normalize lists
└─ Orange Book (49,983 products)
   └─ Clean: Split DF;Route, parse Excel serials, normalize
   
Output: main_table_clean, orange_book_clean
```

**Step 2: NDA-ANDA Matching** (`match.py`)
```
Cleaned Data → Match Algorithm → Candidates → Filter
├─ Extract NDAs & ANDAs from Orange Book
├─ Merge study NDAs with Orange Book
├─ Process strength matching (multi-strength handling)
├─ Prepare normalized fields (ING_KEY, DF_TOK, RT_TOK, STR_N)
├─ Ingredient-based join (Cartesian product)
├─ Apply 3 criteria:
│  ├─ DF_OK: Dosage form tokens overlap
│  ├─ RT_OK: Route tokens overlap
│  └─ STR_OK: Strength exact match
└─ Filter: Keep only DF_OK AND RT_OK AND STR_OK

Output: MatchData object with ~9,034 product-level matches
```

**Step 3: Company Validation** (`postprocess.py` → uses `drugs_api.py` + `extract_from_pdf.py`)
```
Matches → FDA API → PDF Extraction → Validation
├─ Get NDA companies from Orange Book
├─ Query FDA API for ANDA approval letter URLs (drugs_api.py)
│  └─ Rate-limited requests (0.5s delay)
├─ Extract company references from PDFs (extract_from_pdf.py)
│  ├─ Download PDFs (1.0s rate limit)
│  ├─ Parse with PyPDF2
│  └─ Extract bioequivalence statements with company names
├─ Calculate text similarity (company name matching)
│  └─ 90% threshold for validation
└─ Classify matches:
   ├─ Validated: Company match found (≥90% similarity)
   ├─ Rejected: Company conflict (<20% similarity)
   └─ Unknown: Insufficient data (keep conservatively)

Output: validated_matches, rejected_matches, validation_details
```

**Step 4: Monopoly Time Calculation** (`postprocess.py`)
```
Validated Matches → Aggregate → Calculate
├─ Filter: ANDAs approved after NDA
├─ Find earliest ANDA per NDA
├─ Calculate days: (earliest_anda_date - nda_date)
├─ Convert to years: days / 365.25
└─ Compare: actual vs granted monopoly

Output: nda_monopoly_times DataFrame
```

**Step 5: Visualization** (`monopoly_time.py`)
```
Monopoly Times → Plot → Interactive HTML
├─ Filter: NDAs with both actual and granted times
├─ Create hover text (company, dates, ANDAs)
├─ Color code:
│  ├─ Blue: Actual < Granted (early competition)
│  └─ Orange: Actual ≥ Granted (delayed competition)
├─ Generate Plotly scatter plot
└─ Save as interactive HTML

Output: nda_monopoly_times_plot.html
```

## Module Descriptions

### Core Pipeline Modules

**preprocess.py** - Data Loading & Cleaning
- Loads Excel files (main table + Orange Book)
- Normalizes text (uppercase, squish whitespace, remove brackets)
- Parses dates (handles Excel serials, "Approved Prior to Jan 1, 1982")
- Splits combined "DF;Route" column
- See: `README/preprocess_README.md`

**match.py** - Functional Matching Algorithm
- 3-criteria matching: DF_OK, RT_OK, STR_OK
- Tokenization for flexible matching
- Strength normalization (handles multi-strength products)
- Ingredient-based Cartesian product
- Conservative filtering (all criteria must pass)
- See: `README/match_README.md`

**postprocess.py** - Validation & Analysis
- FDA API integration (via drugs_api.py)
- PDF company extraction (via extract_from_pdf.py)
- Company name similarity matching (90% threshold)
- Conservative validation (keep uncertain matches)
- Monopoly time calculation
- Diagnostic summaries
- See: `README/postprocess_README.md`

**monopoly_time.py** - Interactive Visualization
- Plotly scatter plot (granted vs actual monopoly)
- Rich hover tooltips (company, dates, ANDAs)
- Color-coded categories
- Summary statistics annotation
- HTML export for sharing
- See: `README/monopoly_time_README.md`

### Supporting Modules

**drugs_api.py** - FDA API Client
- DrugsAPI class for FDA API queries
- Multi-ANDA batch processing
- Rate limiting (configurable delays)
- PDF URL extraction from submission records
- Error handling and retries
- See: `README/drugs_api_README.md`

**extract_from_pdf.py** - PDF Company Extraction
- PDFCompanyExtractor for single PDFs
- BatchPDFExtractor for multiple PDFs
- Bioequivalence statement pattern matching
- Company name extraction (4 regex patterns)
- Rate-limited batch processing
- See: `README/extract_from_pdf_README.md`

**match_class.py** - OOP Alternative (Testing Only)
- NDA, ANDA, Match classes
- Object-oriented matching approach
- Built-in validation methods
- NOT used in production (match.py is production)
- See: `README/match_class_README.md`

## Data Sources

### Input Files
- **Main Table**: `Copy of Main Table - Dosage Strength.xlsx`
  - 619 study NDAs with granted monopoly times (MMT_Years)
  - Contains: Appl_No, Ingredient, Approval_Date, DF, Route, Strength, MMT, MMT_Years
  
- **Orange Book**: `OB - Products - Dec 2018.xlsx`
  - 49,983 FDA-approved drug products (NDAs + ANDAs)
  - Contains: Ingredient, DF;Route, Trade_Name, Applicant, Strength, Appl_Type, Appl_No, Product_No, TE_Code, Approval_Date, RLD, RS, Type

### FDA Data (2025 Pipeline)
- **Products.txt**: 49,983 products
- **Applications.txt**: 5,786 NDAs
- **Submissions.txt**: 26,145 applications with approval dates
- Located in: `txts/` directory

### External APIs
- **FDA Drugs@FDA API**: `https://api.fda.gov/drug/drugsfda.json`
- **API Key**: `VVwIr3zalK3V1THgAW2mym0DndMnlMBw1oBegvg6`
- **Approval Letter PDFs**: `http://www.accessdata.fda.gov/drugsatfda_docs/appletter/`

## Results Summary (Typical Run)

### Matching Statistics
- **Study NDAs**: 619
- **NDA Products**: 1,157 (from Orange Book merge)
- **Total Candidates**: ~10,000 (ingredient-matched pairs)
- **Final Matches**: ~9,034 (after 3-criteria filter)
- **Unique ANDAs**: ~2,996

### Validation Statistics
- **FDA API Success**: 40-60% (PDFs accessible)
- **PDF Extraction Success**: 30-50% (company references found)
- **Validated Matches**: ~2,340 (confirmed or unknown)
- **Rejected Matches**: ~200 (confirmed company conflicts)
- **NDAs with Matches**: ~306

### Monopoly Time Analysis
- **Average Granted Monopoly**: 3.0-3.5 years
- **Average Actual Monopoly**: 8-12 years
- **Shorter than Granted**: ~30% of NDAs
- **Longer than Granted**: ~70% of NDAs

## Requirements

### Python Version
- Python 3.8+

### Core Dependencies
```bash
pip install pandas numpy openpyxl
```

### Visualization
```bash
pip install plotly matplotlib
```

### PDF & API
```bash
pip install requests PyPDF2
```

### Complete Installation
```bash
pip install pandas numpy openpyxl plotly matplotlib requests PyPDF2
```

## Usage

### Basic Usage
```bash
python dosage.py
```

### 2025 Orange Book Pipeline
```bash
python dosage_2025.py
```

### Testing (Limited ANDAs)
Edit `dosage.py` and set:
```python
max_andas_to_process=10  # Limit to 10 ANDAs for testing
```

### Output Files
- `final_nda_anda_matches.txt` - NDA-ANDA match pairs
- `pdf_extraction_status.txt` - PDF extraction diagnostics
- `nda_monopoly_times_plot.html` - Interactive visualization
- `monopoly_times_from_matches.csv` - Monopoly time data

## Project Versions

### v0.0.3 (Current)
- Complete 2025 Orange Book support
- FDA API integration with rate limiting
- PDF company validation
- Conservative match validation
- Interactive Plotly visualizations
- Comprehensive documentation

### v0.0.2
- Added company validation
- FDA API integration
- PDF extraction

### v0.0.1
- Initial release
- Basic matching algorithm
- Excel data loading

## Documentation

Detailed documentation for each module:
- 📘 [preprocess.py](README/preprocess_README.md) - Data loading and cleaning
- 📘 [match.py](README/match_README.md) - Matching algorithm (production)
- 📘 [match_class.py](README/match_class_README.md) - OOP matching (testing)
- 📘 [postprocess.py](README/postprocess_README.md) - Validation and analysis
- 📘 [drugs_api.py](README/drugs_api_README.md) - FDA API client
- 📘 [extract_from_pdf.py](README/extract_from_pdf_README.md) - PDF extraction
- 📘 [monopoly_time.py](README/monopoly_time_README.md) - Visualization

## License
MIT License

## Authors
- **Original Research**: Feldman et al.
- **Implementation**: [Your Name]

## Acknowledgments
- FDA Drugs@FDA API
- FDA Orange Book Database
- PyPDF2 for PDF extraction
- Plotly for interactive visualizations
