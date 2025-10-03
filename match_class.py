"""Class-based matching system for NDA-ANDA analysis."""

from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass

import numpy as np
import pandas as pd

from preprocess import str_squish
from postprocess import (
    extract_anda_pdf_urls,
    extract_company_references_from_pdfs,
    calculate_text_similarity,
    validate_company_matches
)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ANDA:
    """ANDA class containing all parsed data from Orange Book DataFrame row."""
    
    def __init__(self, row_data: pd.Series):
        """Initialize ANDA from Orange Book row data.
        
        Args:
            row_data: Pandas Series containing ANDA data from orange_book_clean
        """
        self._data = row_data.copy()
        # Ensure ANDA number is included
        self.anda_number = str(self._data.get('Appl_No', ''))
        self.name = self.anda_number  # Use ANDA number as name
        
    # Core getter methods for ANDA attributes
    def get_anda_number(self) -> str:
        """Get ANDA application number."""
        return self.anda_number
    
    def get_applicant(self) -> str:
        """Get ANDA applicant/company name."""
        return str(self._data.get('Applicant', ''))
    
    def get_product_number(self) -> str:
        """Get ANDA product number."""
        return str(self._data.get('Product_No', ''))
    
    def get_approval_date(self) -> Optional[datetime]:
        """Get ANDA approval date as datetime object."""
        date_str = self._data.get('Approval_Date')
        if pd.isna(date_str):
            return None
        try:
            return pd.to_datetime(date_str)
        except:
            return None
    
    def get_approval_date_str(self) -> str:
        """Get ANDA approval date as string."""
        return str(self._data.get('Approval_Date', ''))
    
    def get_ingredient(self) -> str:
        """Get active ingredient."""
        return str(self._data.get('Ingredient', ''))
    
    def get_strength(self) -> str:
        """Get strength."""
        return str(self._data.get('Strength', ''))
    
    def get_dosage_form(self) -> str:
        """Get dosage form (DF)."""
        return str(self._data.get('DF', ''))
    
    def get_route(self) -> str:
        """Get route of administration."""
        return str(self._data.get('Route', ''))
    
    def get_trade_name(self) -> str:
        """Get trade name."""
        return str(self._data.get('Trade_Name', ''))
    
    def get_te_code(self) -> str:
        """Get Therapeutic Equivalence (TE) code."""
        return str(self._data.get('TE_Code', ''))
    
    def get_rld(self) -> str:
        """Get Reference Listed Drug (RLD) designation."""
        return str(self._data.get('RLD', ''))
    
    def get_rs(self) -> str:
        """Get Reference Standard (RS) designation.""" 
        return str(self._data.get('RS', ''))
    
    def get_type(self) -> str:
        """Get type designation."""
        return str(self._data.get('Type', ''))
    
    def get_marketing_status(self) -> str:
        """Get marketing status."""
        return str(self._data.get('Marketing_Status', ''))
    
    # Normalized versions for matching
    def get_normalized_ingredient(self) -> str:
        """Get normalized ingredient for matching."""
        ingredient = self.get_ingredient()
        return str_squish(ingredient).upper() if ingredient else ''
    
    def get_normalized_strength(self) -> str:
        """Get normalized strength for matching."""
        strength = self.get_strength()
        if not strength:
            return ''
        # Apply normalization logic from match.py
        text = str(strength).upper()
        text = text.replace(",", "")
        text = re.sub(r"[\[\]'\"]", "", text)
        text = re.sub(r"\s+", "", text)
        text = re.sub(r"MG\.?", "MG", text)
        text = text.replace("MCG", "MCG")
        text = text.replace("ML", "ML")
        return text
    
    def get_dosage_form_tokens(self) -> List[str]:
        """Get dosage form tokens for matching."""
        df = self.get_dosage_form()
        return self._normalize_tokens(df)
    
    def get_route_tokens(self) -> List[str]:
        """Get route tokens for matching."""
        route = self.get_route()
        return self._normalize_tokens(route)
    
    def _normalize_tokens(self, value: str) -> List[str]:
        """Normalize text into tokens for matching."""
        if not value:
            return []
        text = str(value).upper()
        text = re.sub(r"[\[\]'\"]", "", text)
        text = re.sub(r"[^A-Z0-9]+", " ", text)
        text = str_squish(text)
        if not text:
            return []
        tokens = text.split(" ")
        seen = set()
        ordered = []
        for token in tokens:
            if token and token not in seen:
                seen.add(token)
                ordered.append(token)
        return ordered
    
    def __repr__(self) -> str:
        return f"ANDA({self.anda_number}: {self.get_ingredient()} {self.get_strength()})"


class NDA:
    """NDA class containing all NDA data from Orange Book and main table."""
    
    def __init__(self, main_table_row: pd.Series, orange_book_rows: pd.DataFrame = None):
        """Initialize NDA from main table and Orange Book data.
        
        Args:
            main_table_row: Pandas Series containing NDA data from main table
            orange_book_rows: DataFrame containing related Orange Book rows for this NDA
        """
        self._main_data = main_table_row.copy()
        self._ob_data = orange_book_rows if orange_book_rows is not None else pd.DataFrame()
        self.nda_number = str(self._main_data.get('Appl_No', ''))
        self.name = self.nda_number
        
    # Core getter methods for NDA attributes
    def get_nda_number(self) -> str:
        """Get NDA application number."""
        return self.nda_number
    
    def get_applicant(self) -> str:
        """Get NDA applicant/company name from main table."""
        return str(self._main_data.get('Applicant', ''))
    
    def get_companies_from_orange_book(self) -> List[str]:
        """Get all company names for this NDA from Orange Book."""
        if self._ob_data.empty:
            return []
        companies = self._ob_data['Applicant'].dropna().unique().tolist()
        return [str(c) for c in companies if c]
    
    def get_all_companies(self) -> List[str]:
        """Get all company names from both main table and Orange Book."""
        companies = []
        main_applicant = self.get_applicant()
        if main_applicant:
            companies.append(main_applicant)
        
        ob_companies = self.get_companies_from_orange_book()
        for company in ob_companies:
            if company not in companies:
                companies.append(company)
        
        return companies
    
    def get_approval_date(self) -> Optional[datetime]:
        """Get NDA approval date as datetime object."""
        date_str = self._main_data.get('Approval_Date')
        if pd.isna(date_str):
            return None
        try:
            return pd.to_datetime(date_str)
        except:
            return None
    
    def get_approval_date_str(self) -> str:
        """Get NDA approval date as string."""
        return str(self._main_data.get('Approval_Date', ''))
    
    def get_ingredient(self) -> str:
        """Get active ingredient."""
        return str(self._main_data.get('Ingredient', ''))
    
    def get_strength_list(self) -> str:
        """Get strength list from main table."""
        return str(self._main_data.get('Strength', ''))
    
    def get_dosage_form(self) -> str:
        """Get dosage form (DF)."""
        return str(self._main_data.get('DF', ''))
    
    def get_route(self) -> str:
        """Get route of administration."""
        return str(self._main_data.get('Route', ''))
    
    def get_product_count(self) -> int:
        """Get product count."""
        try:
            return int(self._main_data.get('Product_Count', 0))
        except (ValueError, TypeError):
            return 0
    
    def get_strength_count(self) -> int:
        """Get strength count."""
        try:
            return int(self._main_data.get('Strength_Count', 0))
        except (ValueError, TypeError):
            return 0
    
    def get_mmt(self) -> str:
        """Get Market Monopoly Time (MMT) designation."""
        return str(self._main_data.get('MMT', ''))
    
    def get_mmt_years(self) -> float:
        """Get MMT years as float."""
        try:
            return float(self._main_data.get('MMT_Years', 0))
        except (ValueError, TypeError):
            return 0.0
    
    # Orange Book specific getters
    def get_orange_book_products(self) -> pd.DataFrame:
        """Get all Orange Book product rows for this NDA."""
        return self._ob_data.copy()
    
    def get_strengths_from_orange_book(self) -> List[str]:
        """Get all strengths for this NDA from Orange Book."""
        if self._ob_data.empty:
            return []
        strengths = self._ob_data['Strength'].dropna().unique().tolist()
        return [str(s) for s in strengths if s]
    
    def get_trade_names(self) -> List[str]:
        """Get all trade names for this NDA from Orange Book."""
        if self._ob_data.empty:
            return []
        names = self._ob_data['Trade_Name'].dropna().unique().tolist()
        return [str(n) for n in names if n]
    
    # Normalized versions for matching
    def get_normalized_ingredient(self) -> str:
        """Get normalized ingredient for matching."""
        ingredient = self.get_ingredient()
        return str_squish(ingredient).upper() if ingredient else ''
    
    def get_dosage_form_tokens(self) -> List[str]:
        """Get dosage form tokens for matching."""
        df = self.get_dosage_form()
        return self._normalize_tokens(df)
    
    def get_route_tokens(self) -> List[str]:
        """Get route tokens for matching."""
        route = self.get_route()
        return self._normalize_tokens(route)
    
    def _normalize_tokens(self, value: str) -> List[str]:
        """Normalize text into tokens for matching."""
        if not value:
            return []
        text = str(value).upper()
        text = re.sub(r"[\[\]'\"]", "", text)
        text = re.sub(r"[^A-Z0-9]+", " ", text)
        text = str_squish(text)
        if not text:
            return []
        tokens = text.split(" ")
        seen = set()
        ordered = []
        for token in tokens:
            if token and token not in seen:
                seen.add(token)
                ordered.append(token)
        return ordered
    
    def __repr__(self) -> str:
        return f"NDA({self.nda_number}: {self.get_ingredient()} - {self.get_applicant()})"


class Match:
    """Match class containing an NDA object and list of matching ANDAs."""
    
    def __init__(self, nda: NDA, initial_andas: List[ANDA] = None):
        """Initialize Match with NDA and optional initial ANDAs.
        
        Args:
            nda: NDA object
            initial_andas: Optional list of initial ANDA matches
        """
        self.nda = nda
        self._andas = initial_andas if initial_andas else []
        self.name = f"NDA{nda.get_nda_number()}_match"
        
        # Apply impossible match elimination
        self._andas = self.eliminate_impossible_matches(self._andas)
        
    def add_anda(self, anda: ANDA) -> None:
        """Add an ANDA to this match."""
        # Check if it's not an impossible match
        valid_andas = self.eliminate_impossible_matches([anda])
        if valid_andas:
            self._andas.append(anda)
    
    def remove_anda(self, anda_number: str) -> None:
        """Remove an ANDA by number."""
        self._andas = [anda for anda in self._andas if anda.get_anda_number() != anda_number]
    
    def eliminate_impossible_matches(self, andas: List[ANDA]) -> List[ANDA]:
        """Eliminate ANDAs with approval dates before NDA approval date.
        
        Args:
            andas: List of ANDA objects to check
            
        Returns:
            List of ANDAs with valid approval dates (after NDA approval)
        """
        nda_approval = self.nda.get_approval_date()
        if not nda_approval:
            logger.warning(f"No NDA approval date for {self.nda.get_nda_number()}, keeping all ANDAs")
            return andas
        
        valid_andas = []
        eliminated_count = 0
        
        for anda in andas:
            anda_approval = anda.get_approval_date()
            if not anda_approval:
                logger.warning(f"No ANDA approval date for {anda.get_anda_number()}, keeping ANDA")
                valid_andas.append(anda)
                continue
            
            if anda_approval >= nda_approval:
                valid_andas.append(anda)
            else:
                eliminated_count += 1
                logger.info(f"Eliminated impossible match: ANDA {anda.get_anda_number()} "
                          f"approved {anda_approval.strftime('%Y-%m-%d')} before "
                          f"NDA {self.nda.get_nda_number()} approved {nda_approval.strftime('%Y-%m-%d')}")
        
        logger.info(f"Eliminated {eliminated_count} impossible matches for NDA {self.nda.get_nda_number()}")
        return valid_andas
    
    def get_matches(self) -> List[ANDA]:
        """Get list of matching ANDAs."""
        return self._andas.copy()
    
    def get_match_count(self) -> int:
        """Get number of matching ANDAs."""
        return len(self._andas)
    
    def get_match_numbers_in_date_order(self) -> List[str]:
        """Get ANDA numbers in chronological order by approval date."""
        andas_with_dates = []
        andas_without_dates = []
        
        for anda in self._andas:
            approval_date = anda.get_approval_date()
            if approval_date:
                andas_with_dates.append((approval_date, anda.get_anda_number()))
            else:
                andas_without_dates.append(anda.get_anda_number())
        
        # Sort by date
        andas_with_dates.sort(key=lambda x: x[0])
        
        # Return dated ANDAs first, then undated ones
        result = [anda_num for _, anda_num in andas_with_dates]
        result.extend(andas_without_dates)
        
        return result
    
    def verify_matches(self, orange_book_clean: pd.DataFrame, 
                      validation_function: Optional[callable] = None,
                      use_pdf_validation: bool = True) -> 'Match':
        """Verify matches using company validation logic from postprocess.py.
        
        Args:
            orange_book_clean: Orange Book DataFrame for company validation
            validation_function: Optional custom validation function
            use_pdf_validation: Whether to use PDF-based validation (default True)
            
        Returns:
            Self (for method chaining)
        """
        if validation_function:
            self._andas = validation_function(self.nda, self._andas, orange_book_clean)
        elif use_pdf_validation:
            # Use full PDF-based validation from postprocess.py
            self._andas = self._pdf_based_validation(orange_book_clean)
        else:
            # Use conservative validation - only eliminate matches known to be incorrect
            self._andas = self._conservative_validation(orange_book_clean)
        
        return self
    
    def validate_company_matches_api(self, api_key: str = None) -> 'Match':
        """Validate matches using FDA API submission data.
        
        TODO: This method will use get_api_submissions from postprocess.py
        to get PDF URLs for ANDAs and validate company matches.
        
        Args:
            api_key: Optional FDA API key (defaults to key in postprocess.py)
            
        Returns:
            Self (for method chaining)
        """
        # TODO: Implement API-based company validation
        # This will:
        # 1. Call get_api_submissions with self._andas
        # 2. Extract company references from found PDFs
        # 3. Validate matches using company name matching
        # 4. Update self._andas to remove invalid matches
        
        logger.warning(f"API-based validation not yet implemented for NDA {self.nda.get_nda_number()}")
        logger.info(f"Keeping all {len(self._andas)} matches (placeholder implementation)")
        
        return self
    
    def _pdf_based_validation(self, orange_book_clean: pd.DataFrame) -> List[ANDA]:
        """Full PDF-based validation using postprocess.py logic.
        
        This method:
        1. Creates a temporary DataFrame with NDA-ANDA matches
        2. Extracts PDF URLs for ANDA approval letters  
        3. Parses PDFs to extract company references
        4. Validates matches using company name matching
        5. Returns only validated ANDAs (conservative approach)
        """
        if not self._andas:
            logger.info(f"No ANDAs to validate for NDA {self.nda.get_nda_number()}")
            return []
        
        logger.info(f"Starting PDF-based validation for NDA {self.nda.get_nda_number()} with {len(self._andas)} ANDAs")
        
        # Step 1: Create temporary DataFrame for validation
        match_rows = []
        for anda in self._andas:
            match_row = {
                'NDA_Appl_No': self.nda.get_nda_number(),
                'ANDA_Appl_No': anda.get_anda_number(),
                'ANDA_Approval_Date_Date': anda.get_approval_date()
            }
            match_rows.append(match_row)
        
        anda_matches_df = pd.DataFrame(match_rows)
        
        # Step 2: Get NDA companies
        nda_companies = {self.nda.get_nda_number(): self.nda.get_all_companies()}
        
        if not nda_companies[self.nda.get_nda_number()]:
            logger.warning(f"No company data for NDA {self.nda.get_nda_number()}, keeping all matches")
            return self._andas
        
        try:
            # Step 3: Extract ANDA PDF URLs
            logger.info(f"Extracting PDF URLs for {len(self._andas)} ANDAs...")
            anda_pdf_urls = extract_anda_pdf_urls(anda_matches_df, test_urls=True)
            logger.info(f"Found {len(anda_pdf_urls)} working PDF URLs")
            
            # Step 4: Extract company references from PDFs
            if anda_pdf_urls:
                logger.info("Extracting company references from PDFs...")
                company_references = extract_company_references_from_pdfs(anda_pdf_urls)
                logger.info(f"Extracted references from {len([ref for ref in company_references.values() if ref])} PDFs")
            else:
                logger.warning("No PDF URLs found, skipping PDF validation")
                company_references = {}
            
            # Step 5: Validate matches using 90% similarity threshold
            logger.info("Validating matches using company references...")
            validated_matches, rejected_matches = validate_company_matches(
                nda_companies, company_references, anda_matches_df, similarity_threshold=0.9
            )
            
            # Step 6: Filter ANDAs to keep only validated ones
            if not validated_matches.empty:
                validated_anda_numbers = set(validated_matches['ANDA_Appl_No'].astype(str))
                validated_andas = [anda for anda in self._andas 
                                 if anda.get_anda_number() in validated_anda_numbers]
            else:
                # If no matches were validated, use conservative approach and keep all
                # (this happens when PDFs aren't accessible)
                logger.warning(f"No matches validated for NDA {self.nda.get_nda_number()}, keeping all matches conservatively")
                validated_andas = self._andas
            
            # Log rejection details
            if not rejected_matches.empty:
                rejected_anda_numbers = set(rejected_matches['ANDA_Appl_No'].astype(str))
                rejected_count = len([anda for anda in self._andas 
                                    if anda.get_anda_number() in rejected_anda_numbers])
                logger.info(f"✗ Rejected {rejected_count} matches for NDA {self.nda.get_nda_number()} due to company conflicts")
            
            logger.info(f"PDF validation kept {len(validated_andas)}/{len(self._andas)} matches for NDA {self.nda.get_nda_number()}")
            return validated_andas
            
        except Exception as e:
            logger.error(f"Error during PDF validation for NDA {self.nda.get_nda_number()}: {str(e)}")
            logger.warning("Falling back to conservative validation due to error")
            return self._conservative_validation(orange_book_clean)
    
    def _conservative_validation(self, orange_book_clean: pd.DataFrame) -> List[ANDA]:
        """Conservative validation that only eliminates known incorrect matches.
        
        This is a fallback method that keeps all matches when PDF validation
        is not available or fails. Based on postprocess.py validation logic.
        """
        # Get NDA companies
        nda_companies = self.nda.get_all_companies()
        
        if not nda_companies:
            logger.info(f"No company data for NDA {self.nda.get_nda_number()}, keeping all matches")
            return self._andas
        
        # Conservative approach: keep all matches unless we have definitive evidence they are wrong
        # Since this is the fallback method, we don't have PDF data to make definitive determinations
        logger.info(f"Conservative validation kept {len(self._andas)}/{len(self._andas)} "
                   f"matches for NDA {self.nda.get_nda_number()} (no PDF validation available)")
        
        return self._andas
    
    def get_validation_summary(self) -> Dict[str, Any]:
        """Get summary of validation status for this match.
        
        Returns:
            Dictionary with validation details
        """
        return {
            'nda_number': self.nda.get_nda_number(),
            'nda_companies': self.nda.get_all_companies(),
            'total_andas': len(self._andas),
            'anda_numbers': [anda.get_anda_number() for anda in self._andas],
            'validation_method': 'PDF-based' if hasattr(self, '_last_validation_method') else 'Unknown'
        }
    
    def calculate_monopoly_time(self) -> Optional[float]:
        """Calculate monopoly time in years from NDA approval to first ANDA approval.
        
        Returns:
            Monopoly time in years, or None if no valid ANDAs
        """
        if not self._andas:
            return None
        
        nda_approval = self.nda.get_approval_date()
        if not nda_approval:
            logger.warning(f"No NDA approval date for {self.nda.get_nda_number()}")
            return None
        
        # Find earliest ANDA approval date
        earliest_anda_approval = None
        earliest_anda_number = None
        
        for anda in self._andas:
            anda_approval = anda.get_approval_date()
            if anda_approval:
                if earliest_anda_approval is None or anda_approval < earliest_anda_approval:
                    earliest_anda_approval = anda_approval
                    earliest_anda_number = anda.get_anda_number()
        
        if not earliest_anda_approval:
            logger.warning(f"No ANDA approval dates found for NDA {self.nda.get_nda_number()}")
            return None
        
        # Calculate monopoly time in years
        monopoly_days = (earliest_anda_approval - nda_approval).days
        monopoly_years = monopoly_days / 365.25  # Account for leap years
        
        logger.info(f"NDA {self.nda.get_nda_number()}: Monopoly time = {monopoly_years:.2f} years "
                   f"(until ANDA {earliest_anda_number} on {earliest_anda_approval.strftime('%Y-%m-%d')})")
        
        return monopoly_years
    
    def get_monopoly_summary(self) -> Dict[str, Any]:
        """Get comprehensive monopoly time summary.
        
        Returns:
            Dictionary with monopoly time analysis details
        """
        nda_approval = self.nda.get_approval_date()
        monopoly_years = self.calculate_monopoly_time()
        granted_years = self.nda.get_mmt_years()
        
        summary = {
            'nda_number': self.nda.get_nda_number(),
            'nda_approval_date': nda_approval.strftime('%Y-%m-%d') if nda_approval else None,
            'nda_company': self.nda.get_applicant(),
            'ingredient': self.nda.get_ingredient(),
            'granted_monopoly_years': granted_years,
            'actual_monopoly_years': monopoly_years,
            'shorter_than_granted': monopoly_years < granted_years if (monopoly_years and granted_years) else None,
            'matching_anda_count': len(self._andas),
            'matching_anda_numbers': self.get_match_numbers_in_date_order()
        }
        
        return summary
    
    def __repr__(self) -> str:
        return f"Match({self.name}: {len(self._andas)} ANDAs)"