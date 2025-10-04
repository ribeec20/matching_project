# NDA-ANDA Matching Project

## Overview
This project analyzes pharmaceutical NDA (New Drug Application) and ANDA (Abbreviated New Drug Application) data to calculate monopoly times - the period between NDA approval and the first generic ANDA approval.

## Features
- **Data Loading & Preprocessing**: Clean and prepare FDA Orange Book data and main study data
- **NDA-ANDA Matching**: Match generic drugs (ANDAs) to their reference branded drugs (NDAs) based on:
  - Active ingredient
  - Dosage form
  - Route of administration
  - Strength
- **PDF Validation**: Validate matches by extracting company information from FDA approval letters
- **FDA API Integration**: Retrieve submission documents and approval letters via FDA API
- **Monopoly Time Calculation**: Calculate actual monopoly periods and compare to granted exclusivity
- **Visualization**: Generate scatter plots and analysis of monopoly time patterns

## Project Structure
```
├── dosage.py                 # Main entry point
├── preprocess.py             # Data cleaning and preprocessing
├── load_data.py              # Load data and create NDA/ANDA objects
├── match_new.py              # Core matching logic
├── match_class.py            # NDA, ANDA, Match, and NDAANDAMatcher classes
├── postprocess.py            # PDF validation and monopoly time calculations
├── monopoly_time.py          # Visualization and analysis
├── extract_from_pdf.py       # PDF text extraction utilities
├── test_api_submissions.py   # API testing utilities
└── txts/                     # FDA data text files
```

## Data Sources
- **Main Table**: Study NDAs with market monopoly time grants (205 NDAs)
- **FDA Orange Book**: Approved drug products (16,510 ANDAs)
- **FDA API**: Approval letter PDFs and submission documents

## Key Classes
- `NDA`: Represents New Drug Applications (branded drugs)
- `ANDA`: Represents Abbreviated New Drug Applications (generic drugs)
- `Match`: Represents a validated NDA-ANDA match pair
- `NDAANDAMatcher`: Orchestrates the matching process

## Workflow
1. Load and preprocess data from Excel files and FDA text files
2. Create NDA and ANDA objects from cleaned data
3. Match ANDAs to NDAs based on therapeutic equivalence
4. Validate matches using PDF approval letters and company names
5. Calculate monopoly times (time from NDA to first ANDA approval)
6. Generate visualizations and statistical summaries

## Requirements
- Python 3.x
- pandas
- numpy
- matplotlib
- plotly
- requests
- PyPDF2

## Usage
```python
python dosage.py
```

## Version
v0.0.1 - Initial release

## FDA API
API Key: VVwIr3zalK3V1THgAW2mym0DndMnlMBw1oBegvg6
Base URL: https://api.fda.gov/drug/drugsfda.json

## License
MIT License
