"""Post-processing utilities for NDA/ANDA matching diagnostics."""

from __future__ import annotations

from typing import Dict, List, Optional, Tuple
import time
import logging

import numpy as np
import pandas as pd

from match import MatchData
from extract_from_pdf import BatchPDFExtractor
from drugs_api import DrugsAPI

# Set up logging for the validation process
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_api_submissions(anda_objects: List) -> Dict[str, Optional[str]]:
    """Get PDF URLs for ANDA approval letters from FDA API.
    
    This function queries the FDA API to find approval letter PDFs for ANDA applications.
    It uses the DrugsAPI class to handle all API interactions.
    
    Args:
        anda_objects: List of ANDA objects with get_anda_number() method
        
    Returns:
        Dictionary mapping ANDA number to PDF URL (or None if not found)
    """
    api_client = DrugsAPI()
    return api_client.get_multiple_anda_pdfs(anda_objects, rate_limit_delay=0.5)


def get_nda_companies_from_main_table(nda_numbers: List[str], main_table_clean: pd.DataFrame, orange_book_clean: pd.DataFrame) -> Dict[str, List[str]]:
    """Get company names for each NDA number, prioritizing NDAs from main table but getting companies from Orange Book.
    
    Args:
        nda_numbers: List of NDA application numbers
        main_table_clean: Main table DataFrame (contains the NDAs we're interested in)
        orange_book_clean: Orange Book DataFrame (contains company information)
        
    Returns:
        Dictionary mapping NDA number to list of company names
    """
    nda_companies = {}
    
    # Filter Orange Book for NDAs only  
    nda_records = orange_book_clean[orange_book_clean['Appl_Type'] == 'N'].copy()
    
    # Only get companies for NDAs that are in our main table
    main_table_ndas = set(main_table_clean['Appl_No'].astype(str))
    
    for nda_num in nda_numbers:
        # Only process if this NDA is in our main table
        if nda_num in main_table_ndas:
            # Find all companies for this NDA in Orange Book
            nda_rows = nda_records[nda_records['Appl_No'] == nda_num]
            
            if not nda_rows.empty:
                # Get unique company names from Orange Book
                companies = nda_rows['Applicant'].dropna().unique().tolist()
                nda_companies[nda_num] = companies
            else:
                nda_companies[nda_num] = []
                logger.warning(f"No company found for NDA {nda_num} in Orange Book")
        else:
            # This NDA is not in our main table, skip it
            logger.info(f"NDA {nda_num} not in main table, skipping")
            nda_companies[nda_num] = []
    
    return nda_companies


def extract_company_references_from_pdfs(anda_pdf_urls: Dict[str, str]) -> Dict[str, Optional[str]]:
    """Extract company reference text from ANDA approval letter PDFs.
    
    Args:
        anda_pdf_urls: Dictionary mapping ANDA number to PDF URL
        
    Returns:
        Dictionary mapping ANDA number to extracted company reference text
    """
    batch_extractor = BatchPDFExtractor(rate_limit_delay=1.0)
    return batch_extractor.extract_companies_from_andas(anda_pdf_urls)


def calculate_text_similarity(company_name: str, reference_text: str) -> float:
    """Calculate similarity between company name and reference text.
    
    Args:
        company_name: Company name to search for
        reference_text: Text to search in
        
    Returns:
        Similarity score between 0 and 1 (1 = perfect match)
    """
    if not company_name or not reference_text:
        return 0.0
    
    company_upper = company_name.upper()
    text_upper = reference_text.upper()
    
    # Split company name into words (ignore short words like "INC", "LLC")
    company_words = [word for word in company_upper.split() if len(word) > 3]
    
    if not company_words:
        # For short company names, use exact matching
        return 1.0 if company_upper in text_upper else 0.0
    
    # Count how many significant words from company name appear in text
    matched_words = sum(1 for word in company_words if word in text_upper)
    
    # Calculate percentage match
    return matched_words / len(company_words)


def validate_company_matches(
    nda_companies: Dict[str, List[str]], 
    company_references: Dict[str, Optional[str]],
    anda_matches: pd.DataFrame,
    similarity_threshold: float = 0.9
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Validate NDA-ANDA matches by checking if NDA companies appear in ANDA approval letters.
    
    Conservative approach: Only reject matches when we have positive evidence they are incorrect.
    Matches with missing data (no PDF, no company info) are kept, not rejected.
    
    Args:
        nda_companies: Dictionary mapping NDA number to list of company names
        company_references: Dictionary mapping ANDA number to extracted reference text
        anda_matches: DataFrame containing NDA-ANDA matches
        similarity_threshold: Minimum similarity score for company matching (default 0.9 = 90%)
        
    Returns:
        Tuple of (validated_matches, rejected_matches) DataFrames
        - validated_matches: Includes confirmed matches AND unverifiable matches (conservative)
        - rejected_matches: Only matches with confirmed conflicts
    """
    validated_matches = []
    rejected_matches = []
    
    for _, match_row in anda_matches.iterrows():
        nda_num = str(match_row['NDA_Appl_No'])
        anda_num = str(match_row['ANDA_Appl_No'])
        
        # Get NDA companies and ANDA reference text
        nda_company_list = nda_companies.get(nda_num, [])
        anda_ref_text = company_references.get(anda_num)
        
        # Check if any NDA company appears in the ANDA reference text
        company_found = False
        matched_company = None
        best_similarity = 0.0
        
        if anda_ref_text and nda_company_list:
            anda_ref_upper = anda_ref_text.upper()
            
            for company in nda_company_list:
                if company and isinstance(company, str):
                    # Strategy 1: Exact company name matching (case-insensitive)
                    company_upper = company.upper()
                    if company_upper in anda_ref_upper:
                        company_found = True
                        matched_company = company
                        best_similarity = 1.0
                        break
                    
                    # Strategy 2: 90% similarity matching
                    similarity = calculate_text_similarity(company, anda_ref_text)
                    if similarity >= similarity_threshold and similarity > best_similarity:
                        company_found = True
                        matched_company = company
                        best_similarity = similarity
        
        # Add validation results to the match row
        match_row_copy = match_row.copy()
        match_row_copy['NDA_Companies'] = ' | '.join(nda_company_list) if nda_company_list else 'Unknown'
        match_row_copy['Company_Match_Found'] = company_found
        match_row_copy['Matched_Company'] = matched_company if matched_company else 'None'
        match_row_copy['Company_Match_Similarity'] = best_similarity
        
        # Determine validation status more conservatively
        validation_status = 'Unknown'  # Default status
        
        if company_found:
            # We found a match - this is validated
            validation_status = 'Validated'
            validated_matches.append(match_row_copy)
            logger.info(f"✓ Validated: NDA {nda_num} - ANDA {anda_num} (Company: {matched_company}, Similarity: {best_similarity:.2f})")
        elif anda_ref_text and nda_company_list:
            # We have both PDF text and NDA companies, but no match found
            # Apply more rigorous rejection criteria for ANDAs with clear conflicts
            
            max_similarity = 0.0
            for company in nda_company_list:
                if company and isinstance(company, str):
                    similarity = calculate_text_similarity(company, anda_ref_text)
                    max_similarity = max(max_similarity, similarity)
            
            # More aggressive rejection: if we have PDF text and company data,
            # but very low similarity, this is likely an incorrect match
            if max_similarity < 0.2:  # Less than 20% similarity indicates likely mismatch
                validation_status = 'Rejected'
                rejected_matches.append(match_row_copy)
                logger.warning(f"✗ Rejected: NDA {nda_num} - ANDA {anda_num} (Low company similarity: {max_similarity:.2f})")
            else:
                # Some similarity found, be conservative and keep the match
                validation_status = 'Unknown'
                validated_matches.append(match_row_copy)
                logger.info(f"? Keeping: NDA {nda_num} - ANDA {anda_num} (Marginal similarity: {max_similarity:.2f})")
                
        else:
            # Missing PDF text or NDA companies - we can't validate but shouldn't reject
            validation_status = 'Unknown'
            validated_matches.append(match_row_copy)  # Keep the match
            if not anda_ref_text:
                logger.info(f"? Keeping: NDA {nda_num} - ANDA {anda_num} (No PDF text available)")
            elif not nda_company_list:
                logger.info(f"? Keeping: NDA {nda_num} - ANDA {anda_num} (No NDA company data)")
            else:
                logger.info(f"? Keeping: NDA {nda_num} - ANDA {anda_num} (Insufficient data for validation)")
        
        match_row_copy['Validation_Status'] = validation_status
    
    validated_df = pd.DataFrame(validated_matches) if validated_matches else pd.DataFrame()
    rejected_df = pd.DataFrame(rejected_matches) if rejected_matches else pd.DataFrame()
    
    # DEBUG: Check what columns are in the validated matches
    if not validated_df.empty:
        print(f"\n[DEBUG] Validated matches columns: {validated_df.columns.tolist()}")
        print(f"   Has ANDA_Approval_Date_Date: {'ANDA_Approval_Date_Date' in validated_df.columns}")
        if 'ANDA_Approval_Date_Date' in validated_df.columns:
            print(f"   Sample ANDA dates: {validated_df['ANDA_Approval_Date_Date'].head(3).tolist()}")
    
    return validated_df, rejected_df


def nda_anda_company_validation(
    match_data: MatchData, 
    orange_book_clean: pd.DataFrame,
    main_table_clean: pd.DataFrame,
    max_andas_to_process: Optional[int] = None
) -> Tuple[pd.DataFrame, pd.DataFrame, Dict]:
    """Main function to validate NDA-ANDA matches using company reference validation.
    
    Uses conservative validation: only rejects matches with confirmed conflicts.
    Matches without sufficient data for validation are kept, not discarded.
    
    Args:
        match_data: MatchData object containing NDA-ANDA matches
        orange_book_clean: Orange Book DataFrame
        main_table_clean: Main table DataFrame (primary source for NDA companies)
        max_andas_to_process: Optional limit on number of ANDAs to process (for testing)
        
    Returns:
        Tuple of (validated_matches, rejected_matches, validation_details)
        - validated_matches: Confirmed good matches + unverifiable matches (conservative approach)
        - rejected_matches: Only matches with confirmed company conflicts
    """
    logger.info("Starting NDA-ANDA company validation process...")
    
    # Step 1: Get unique NDA numbers from matches
    nda_numbers = match_data.anda_matches['NDA_Appl_No'].dropna().astype(str).unique().tolist()
    logger.info(f"Found {len(nda_numbers)} unique NDAs to validate")
    
    # Step 2: Get NDA companies from main table (primary source)
    logger.info("Retrieving NDA companies for main table NDAs...")
    nda_companies = get_nda_companies_from_main_table(nda_numbers, main_table_clean, orange_book_clean)
    
    # Step 3: Limit processing for testing if specified
    anda_matches_to_process = match_data.anda_matches.copy()
    if max_andas_to_process:
        unique_andas = anda_matches_to_process['ANDA_Appl_No'].dropna().unique()
        limited_andas = unique_andas[:max_andas_to_process]
        anda_matches_to_process = anda_matches_to_process[
            anda_matches_to_process['ANDA_Appl_No'].isin(limited_andas)
        ]
        logger.info(f"Limited processing to {len(limited_andas)} ANDAs for testing")
    
    # Step 4: Extract ANDA PDF URLs using FDA API
    logger.info("Querying FDA API for ANDA approval letter URLs...")
    
    # Create simple ANDA objects for API querying
    class SimpleANDA:
        def __init__(self, anda_num):
            self.anda_num = str(anda_num)
        def get_anda_number(self):
            return self.anda_num
    
    unique_anda_numbers = anda_matches_to_process['ANDA_Appl_No'].dropna().unique()
    anda_objects = [SimpleANDA(anda_num) for anda_num in unique_anda_numbers]
    
    # Use DrugsAPI to get PDF URLs
    anda_pdf_urls = get_api_submissions(anda_objects)
    
    # Filter out None values
    anda_pdf_urls = {k: v for k, v in anda_pdf_urls.items() if v is not None}
    
    logger.info(f"Found {len(anda_pdf_urls)} ANDA PDF URLs from FDA API")
    
    # Step 5: Extract company references from PDFs
    logger.info("Extracting company references from ANDA approval letters...")
    company_references = extract_company_references_from_pdfs(anda_pdf_urls)
    
    # Step 6: Validate matches using 90% similarity threshold
    logger.info("Validating NDA-ANDA matches based on company references...")
    
    # DEBUG: Check input to validation
    print(f"\n[DEBUG] Input to validate_company_matches:")
    print(f"   anda_matches_to_process columns: {anda_matches_to_process.columns.tolist()}")
    print(f"   Has ANDA_Approval_Date_Date: {'ANDA_Approval_Date_Date' in anda_matches_to_process.columns}")
    
    validated_matches, rejected_matches = validate_company_matches(
        nda_companies, company_references, anda_matches_to_process, similarity_threshold=0.9
    )
    
    # Step 7: Compile validation details
    validation_details = {
        'nda_companies': nda_companies,
        'anda_pdf_urls': anda_pdf_urls,
        'company_references': company_references,
        'total_matches_processed': len(anda_matches_to_process),
        'validated_count': len(validated_matches),
        'rejected_count': len(rejected_matches)
    }
    
    logger.info(f"Validation complete: {len(validated_matches)} validated, {len(rejected_matches)} rejected")
    
    return validated_matches, rejected_matches, validation_details





def compute_strength_summary(study_ndas_strength: pd.DataFrame) -> pd.DataFrame:
    """Summarize how NDA strengths matched Orange Book strengths."""
    matches = study_ndas_strength.get("strength_match", pd.Series(dtype=bool)).fillna(False)
    total = len(study_ndas_strength)
    return pd.DataFrame(
        {
            "rows": [total],
            "strength_match_true": [int(matches.sum())],
            "strength_match_false": [int((~matches).sum())],
        }
    )


def compute_date_summary(date_check: pd.DataFrame) -> pd.DataFrame:
    """Summarize NDA approval date alignment between data sources."""
    both_non_na = date_check.get("both_non_na")
    date_equal = date_check.get("date_equal")
    date_diff = date_check.get("date_diff_days")

    if both_non_na is None:
        both_non_na = pd.Series(False, index=date_check.index)
    if date_equal is None:
        date_equal = pd.Series(False, index=date_check.index)
    if date_diff is None:
        date_diff = pd.Series(np.nan, index=date_check.index)

    diff_values = date_diff.to_numpy(dtype=float)
    if diff_values.size == 0 or np.isnan(diff_values).all():
        median_diff = np.nan
        p90_diff = np.nan
    else:
        median_diff = float(np.nanmedian(diff_values))
        p90_diff = float(np.nanpercentile(diff_values, 90))

    return pd.DataFrame(
        {
            "ndas": [len(date_check)],
            "both_dates": [int(both_non_na.sum())],
            "exact_same": [int(date_equal.sum())],
            "diff_nonzero": [int((both_non_na & ~date_equal).sum())],
            "na_in_either": [int((~both_non_na).sum())],
            "median_diff": [median_diff],
            "p90_diff": [p90_diff],
        }
    )


def create_validated_match_data(
    original_match_data: MatchData,
    validated_matches: pd.DataFrame
) -> MatchData:
    """Create a new MatchData object using only validated matches.
    
    Args:
        original_match_data: Original MatchData object
        validated_matches: DataFrame containing only validated NDA-ANDA matches
        
    Returns:
        New MatchData object with filtered matches
    """
    # Create a copy of the original match data
    validated_match_data = MatchData(
        study_ndas=original_match_data.study_ndas,
        study_ndas_strength=original_match_data.study_ndas_strength,
        ndas_ob=original_match_data.ndas_ob,
        andas_ob=original_match_data.andas_ob,
        study_ndas_final=original_match_data.study_ndas_final,
        candidates=original_match_data.candidates,
        anda_matches=validated_matches,  # Use only validated matches
        nda_summary=original_match_data.nda_summary,
        ob_nda_first=original_match_data.ob_nda_first,
        date_check=original_match_data.date_check,
    )
    
    return validated_match_data


def calculate_nda_monopoly_times_with_validation(
    match_data: MatchData,
    validation_status_map: Optional[Dict[str, str]] = None
) -> pd.DataFrame:
    """Calculate monopoly times at the NDA level with validation status tracking.
    
    Args:
        match_data: MatchData object containing NDA-ANDA matches
        validation_status_map: Optional mapping of ANDA numbers to validation status
        
    Returns:
        DataFrame with monopoly times and validation status
    """
    # Get NDA-level data
    nda_summary = match_data.nda_summary.copy()
    
    # Filter matches to only include ANDAs approved after NDA approval
    valid_matches = match_data.anda_matches.copy()
    valid_matches = valid_matches.dropna(subset=["ANDA_Approval_Date_Date"])
    
    # Merge to get NDA approval dates for comparison
    valid_matches = valid_matches.merge(
        nda_summary[["NDA_Appl_No", "NDA_Approval_Date_Date"]], 
        on="NDA_Appl_No", 
        how="left"
    )
    
    # Filter out ANDAs approved before or on the same day as NDA
    valid_matches = valid_matches[
        valid_matches["ANDA_Approval_Date_Date"] > valid_matches["NDA_Approval_Date_Date"]
    ]
    
    if valid_matches.empty:
        # No valid matches, return NDAs with no monopoly end dates
        nda_monopoly = nda_summary.copy()
        nda_monopoly["Earliest_ANDA_Date"] = pd.NaT
        nda_monopoly["Actual_Monopoly_Days"] = np.nan
        nda_monopoly["Actual_Monopoly_Years"] = np.nan
        nda_monopoly["Monopoly_Shorter_Than_Granted"] = np.nan
        nda_monopoly["Num_Matching_ANDAs"] = 0
        nda_monopoly["Validation_Status"] = "No_Matches"
        return nda_monopoly
    
    # Find earliest ANDA per NDA
    earliest_anda_per_nda = (
        valid_matches.groupby("NDA_Appl_No")["ANDA_Approval_Date_Date"]
        .min()
        .rename("Earliest_ANDA_Date")
        .reset_index()
    )
    
    # Count matching ANDAs per NDA and get validation status
    agg_dict = {
        "ANDA_Appl_No": [
            lambda x: x.nunique(),  # Count unique ANDAs
            lambda x: " | ".join(sorted(set(x.astype(str))))  # List of ANDA numbers
        ]
    }
    
    # Only add validation status aggregation if the column exists
    if "Validation_Status" in valid_matches.columns:
        agg_dict["Validation_Status"] = lambda x: "Validated" if (x == "Validated").any() else "Not_Validated"
    
    anda_details = (
        valid_matches.groupby("NDA_Appl_No")
        .agg(agg_dict)
        .reset_index()
    )
    
    # Flatten column names
    if "Validation_Status" in valid_matches.columns:
        anda_details.columns = ["NDA_Appl_No", "Num_Matching_ANDAs", "Matching_ANDA_List", "Validation_Status"]
    else:
        anda_details.columns = ["NDA_Appl_No", "Num_Matching_ANDAs", "Matching_ANDA_List"]
        anda_details["Validation_Status"] = "No_Validation_Data"
    
    # Merge back to NDA summary
    nda_monopoly = (
        nda_summary
        .merge(earliest_anda_per_nda, on="NDA_Appl_No", how="left")
        .merge(anda_details, on="NDA_Appl_No", how="left")
    )
    
    # Fill missing values
    nda_monopoly["Num_Matching_ANDAs"] = nda_monopoly["Num_Matching_ANDAs"].fillna(0).astype(int)
    nda_monopoly["Matching_ANDA_List"] = nda_monopoly["Matching_ANDA_List"].fillna("")
    nda_monopoly["Validation_Status"] = nda_monopoly["Validation_Status"].fillna("No_Matches")
    
    # Calculate monopoly times
    nda_monopoly["Actual_Monopoly_Days"] = (
        nda_monopoly["Earliest_ANDA_Date"] - nda_monopoly["NDA_Approval_Date_Date"]
    ).dt.days
    
    nda_monopoly["Actual_Monopoly_Years"] = nda_monopoly["Actual_Monopoly_Days"] / 365.25
    
    # DEBUG: Print sample calculations
    print("\n[DEBUG] Monopoly time calculations (first 5 NDAs with matches):")
    debug_cols = ['NDA_Appl_No', 'NDA_Approval_Date_Date', 'Earliest_ANDA_Date', 
                  'Actual_Monopoly_Days', 'Actual_Monopoly_Years', 'NDA_MMT_Years']
    available_debug_cols = [col for col in debug_cols if col in nda_monopoly.columns]
    print(nda_monopoly[nda_monopoly['Num_Matching_ANDAs'] > 0][available_debug_cols].head(5))
    print()
    
    # Compare to granted monopoly period
    def shorter_than_granted(row):
        actual = row["Actual_Monopoly_Years"]
        granted = row["NDA_MMT_Years"]
        if pd.isna(actual) or pd.isna(granted):
            return np.nan
        return bool(actual < granted)
    
    nda_monopoly["Monopoly_Shorter_Than_Granted"] = nda_monopoly.apply(shorter_than_granted, axis=1)
    
    return nda_monopoly


def build_postprocess_outputs(match_data: MatchData) -> Dict[str, pd.DataFrame]:
    """Compute all diagnostic DataFrames from the matching stage."""
    strength_summary = compute_strength_summary(match_data.study_ndas_strength)
    date_summary = compute_date_summary(match_data.date_check)
    nda_monopoly_times = calculate_nda_monopoly_times_with_validation(match_data)
    
    # Find NDAs without ANDA matches
    ndas_no_anda = nda_monopoly_times[nda_monopoly_times["Num_Matching_ANDAs"] == 0]

    return {
        "strength_summary": strength_summary,
        "date_summary": date_summary,
        "nda_monopoly_times": nda_monopoly_times,
        "ndas_no_anda": ndas_no_anda,
    }


def display_postprocess_summary(outputs: Dict[str, pd.DataFrame]) -> None:
    """Print a concise summary of diagnostic results."""
    strength_summary = outputs.get("strength_summary")
    date_summary = outputs.get("date_summary")
    nda_monopoly_times = outputs.get("nda_monopoly_times")
    ndas_no_anda = outputs.get("ndas_no_anda")

    if strength_summary is not None:
        print("Strength matching summary:")
        print(strength_summary)
        print()

    if date_summary is not None:
        print("NDA approval date comparison summary:")
        print(date_summary)
        print()

    if nda_monopoly_times is not None:
        total_ndas = len(nda_monopoly_times)
        with_matches = len(nda_monopoly_times[nda_monopoly_times["Num_Matching_ANDAs"] > 0])
        with_monopoly_times = len(nda_monopoly_times[nda_monopoly_times["Actual_Monopoly_Years"].notna()])
        
        print(f"NDA monopoly time summary:")
        print(f"   Total NDAs: {total_ndas}")
        print(f"   NDAs with ANDA matches: {with_matches}")
        print(f"   NDAs with calculated monopoly times: {with_monopoly_times}")
        print()

    if ndas_no_anda is not None:
        print("NDAs with no matched ANDAs (first 10):")
        display_cols = ["NDA_Appl_No", "NDA_Ingredient", "NDA_Approval_Date"]
        available_cols = [col for col in display_cols if col in ndas_no_anda.columns]
        print(ndas_no_anda[available_cols].head(10))
        print()
