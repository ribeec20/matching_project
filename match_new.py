"""Class-based NDA-ANDA matching system."""

from __future__ import annotations

import logging
from typing import Dict, List, Tuple

import pandas as pd

from match_class import NDA, ANDA, Match
from load_data import DataLoader, load_and_create_objects

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class NDAANDAMatcher:
    """Class-based NDA-ANDA matching system."""
    
    def __init__(self, main_table_df: pd.DataFrame, orange_book_df: pd.DataFrame):
        """Initialize matcher with data.
        
        Args:
            main_table_df: Main table DataFrame
            orange_book_df: Orange Book DataFrame
        """
        self.main_table_df = main_table_df
        self.orange_book_df = orange_book_df
        self.nda_objects = {}
        self.anda_objects = {}
        self.match_objects = {}
        self.data_loader = None
        
        # Load and create objects
        self._initialize_objects()
    
    def _initialize_objects(self) -> None:
        """Initialize NDA and ANDA objects from data."""
        logger.info("Initializing NDA and ANDA objects...")
        self.nda_objects, self.anda_objects, self.data_loader = load_and_create_objects(
            self.main_table_df, self.orange_book_df
        )
        logger.info(f"Initialized {len(self.nda_objects)} NDAs and {len(self.anda_objects)} ANDAs")
    
    def _check_ingredient_match(self, nda: NDA, anda: ANDA) -> bool:
        """Check if NDA and ANDA have matching ingredients."""
        nda_ingredient = nda.get_normalized_ingredient()
        anda_ingredient = anda.get_normalized_ingredient()
        return nda_ingredient == anda_ingredient and nda_ingredient != ''
    
    def _check_dosage_form_overlap(self, nda: NDA, anda: ANDA) -> bool:
        """Check if NDA and ANDA dosage forms have overlapping tokens."""
        nda_tokens = nda.get_dosage_form_tokens()
        anda_tokens = anda.get_dosage_form_tokens()
        
        if not nda_tokens or not anda_tokens:
            return False
        
        return bool(set(nda_tokens).intersection(set(anda_tokens)))
    
    def _check_route_overlap(self, nda: NDA, anda: ANDA) -> bool:
        """Check if NDA and ANDA routes have overlapping tokens."""
        nda_tokens = nda.get_route_tokens()
        anda_tokens = anda.get_route_tokens()
        
        if not nda_tokens or not anda_tokens:
            return False
        
        return bool(set(nda_tokens).intersection(set(anda_tokens)))
    
    def _check_strength_match(self, nda: NDA, anda: ANDA) -> bool:
        """Check if NDA and ANDA strengths match.
        
        For NDAs from main table, we need to check if the ANDA strength
        matches any of the strengths from the NDA's Orange Book products.
        """
        anda_strength = anda.get_normalized_strength()
        if not anda_strength:
            return False
        
        # Get NDA strengths from Orange Book
        nda_ob_products = nda.get_orange_book_products()
        if nda_ob_products.empty:
            return False
        
        # Check if ANDA strength matches any NDA Orange Book strength
        for _, nda_product in nda_ob_products.iterrows():
            nda_strength = nda_product.get('Strength', '')
            if not nda_strength:
                continue
            
            # Normalize NDA strength for comparison
            nda_strength_norm = self._normalize_strength(nda_strength)
            if nda_strength_norm == anda_strength:
                return True
        
        return False
    
    def _normalize_strength(self, strength: str) -> str:
        """Normalize strength string for comparison."""
        import re
        if not strength:
            return ''
        
        text = str(strength).upper()
        text = text.replace(",", "")
        text = re.sub(r"[\[\]'\"]", "", text)
        text = re.sub(r"\s+", "", text)
        text = re.sub(r"MG\.?", "MG", text)
        text = text.replace("MCG", "MCG")
        text = text.replace("ML", "ML")
        return text
    
    def find_matches_for_nda(self, nda: NDA) -> List[ANDA]:
        """Find all matching ANDAs for a given NDA using the 3 criteria.
        
        Matching criteria:
        1. Ingredient match (exact)
        2. Dosage form overlap (token-based)
        3. Route overlap (token-based)
        4. Strength match (exact, against Orange Book strengths)
        
        Args:
            nda: NDA object to find matches for
            
        Returns:
            List of matching ANDA objects
        """
        matching_andas = []
        
        logger.debug(f"Finding matches for NDA {nda.get_nda_number()}: {nda.get_ingredient()}")
        
        for anda in self.anda_objects.values():
            # Check all 4 criteria
            if (self._check_ingredient_match(nda, anda) and
                self._check_dosage_form_overlap(nda, anda) and
                self._check_route_overlap(nda, anda) and
                self._check_strength_match(nda, anda)):
                
                matching_andas.append(anda)
                logger.debug(f"  ✓ Match found: ANDA {anda.get_anda_number()}")
        
        logger.info(f"NDA {nda.get_nda_number()}: Found {len(matching_andas)} initial matches")
        return matching_andas
    
    def create_all_matches(self) -> Dict[str, Match]:
        """Create Match objects for all NDAs in the main table.
        
        Returns:
            Dictionary mapping match name to Match object
        """
        logger.info("Creating Match objects for all NDAs...")
        
        self.match_objects = {}
        
        for nda_number, nda in self.nda_objects.items():
            # Find matching ANDAs
            matching_andas = self.find_matches_for_nda(nda)
            
            # Create Match object
            match = Match(nda, matching_andas)
            self.match_objects[match.name] = match
            
            logger.info(f"Created {match.name}: {match.get_match_count()} matches")
        
        logger.info(f"Created {len(self.match_objects)} Match objects")
        return self.match_objects
    
    def verify_all_matches(self, use_company_validation: bool = True, use_pdf_validation: bool = True) -> Dict[str, Match]:
        """Verify all matches using company validation logic.
        
        Args:
            use_company_validation: Whether to apply company validation
            use_pdf_validation: Whether to use PDF-based validation (requires internet access)
            
        Returns:
            Dictionary of verified Match objects
        """
        logger.info("Verifying all matches...")
        
        if not self.match_objects:
            raise ValueError("Match objects must be created before verification")
        
        verified_count = 0
        total_matches_before = 0
        total_matches_after = 0
        pdf_validated_count = 0
        conservative_validated_count = 0
        
        for match_name, match in self.match_objects.items():
            matches_before = match.get_match_count()
            total_matches_before += matches_before
            
            if use_company_validation:
                # Apply verification with PDF validation
                try:
                    match.verify_matches(self.orange_book_df, use_pdf_validation=use_pdf_validation)
                    if use_pdf_validation:
                        pdf_validated_count += 1
                    else:
                        conservative_validated_count += 1
                except Exception as e:
                    logger.warning(f"Validation failed for {match_name}: {str(e)}, using conservative approach")
                    match.verify_matches(self.orange_book_df, use_pdf_validation=False)
                    conservative_validated_count += 1
            
            matches_after = match.get_match_count()
            total_matches_after += matches_after
            
            if matches_after > 0:
                verified_count += 1
            
            logger.debug(f"{match_name}: {matches_before} -> {matches_after} matches")
        
        logger.info(f"Verification complete: {verified_count}/{len(self.match_objects)} matches have ANDAs")
        logger.info(f"Total matches: {total_matches_before} -> {total_matches_after}")
        if use_pdf_validation:
            logger.info(f"PDF validation used for {pdf_validated_count} matches, conservative for {conservative_validated_count}")
        
        return self.match_objects
    
    def calculate_monopoly_times(self) -> List[Dict]:
        """Calculate monopoly times for all matches.
        
        Returns:
            List of monopoly time summary dictionaries
        """
        logger.info("Calculating monopoly times...")
        
        if not self.match_objects:
            raise ValueError("Match objects must be created before calculating monopoly times")
        
        monopoly_summaries = []
        successful_calculations = 0
        
        for match_name, match in self.match_objects.items():
            summary = match.get_monopoly_summary()
            monopoly_summaries.append(summary)
            
            if summary['actual_monopoly_years'] is not None:
                successful_calculations += 1
        
        logger.info(f"Calculated monopoly times for {successful_calculations}/{len(self.match_objects)} matches")
        
        return monopoly_summaries
    
    def get_results_dataframe(self) -> pd.DataFrame:
        """Get results as a comprehensive DataFrame.
        
        Returns:
            DataFrame with all match results and monopoly time calculations
        """
        logger.info("Generating results DataFrame...")
        
        summaries = self.calculate_monopoly_times()
        results_df = pd.DataFrame(summaries)
        
        # Add additional computed columns
        if not results_df.empty:
            # Add year extracted from approval date
            results_df['nda_approval_year'] = pd.to_datetime(
                results_df['nda_approval_date']
            ).dt.year
            
            # Add monopoly time difference
            results_df['monopoly_difference'] = (
                results_df['granted_monopoly_years'] - results_df['actual_monopoly_years']
            )
            
            # Sort by NDA number
            results_df = results_df.sort_values('nda_number').reset_index(drop=True)
        
        logger.info(f"Generated results DataFrame with {len(results_df)} rows")
        return results_df
    
    def get_summary_statistics(self) -> Dict:
        """Get summary statistics for the matching process.
        
        Returns:
            Dictionary with summary statistics
        """
        if not self.match_objects:
            return {}
        
        total_ndas = len(self.match_objects)
        ndas_with_matches = sum(1 for match in self.match_objects.values() if match.get_match_count() > 0)
        total_matches = sum(match.get_match_count() for match in self.match_objects.values())
        
        monopoly_times = []
        granted_times = []
        shorter_than_granted = 0
        
        for match in self.match_objects.values():
            summary = match.get_monopoly_summary()
            if summary['actual_monopoly_years'] is not None:
                monopoly_times.append(summary['actual_monopoly_years'])
            if summary['granted_monopoly_years'] is not None:
                granted_times.append(summary['granted_monopoly_years'])
            if summary['shorter_than_granted']:
                shorter_than_granted += 1
        
        stats = {
            'total_ndas': total_ndas,
            'ndas_with_matches': ndas_with_matches,
            'ndas_without_matches': total_ndas - ndas_with_matches,
            'total_anda_matches': total_matches,
            'average_matches_per_nda': total_matches / total_ndas if total_ndas > 0 else 0,
            'ndas_with_monopoly_calculation': len(monopoly_times),
            'average_monopoly_years': sum(monopoly_times) / len(monopoly_times) if monopoly_times else 0,
            'average_granted_years': sum(granted_times) / len(granted_times) if granted_times else 0,
            'ndas_shorter_than_granted': shorter_than_granted,
            'percentage_shorter_than_granted': (shorter_than_granted / len(monopoly_times) * 100) if monopoly_times else 0
        }
        
        return stats


def run_class_based_matching(main_table_df: pd.DataFrame, 
                           orange_book_df: pd.DataFrame,
                           use_pdf_validation: bool = True) -> Tuple[NDAANDAMatcher, pd.DataFrame]:
    """Run the complete class-based matching process.
    
    Args:
        main_table_df: Main table DataFrame
        orange_book_df: Orange Book DataFrame
        use_pdf_validation: Whether to use PDF-based validation (requires internet access)
        
    Returns:
        Tuple of (matcher_object, results_dataframe)
    """
    logger.info("Starting class-based NDA-ANDA matching process...")
    
    # Initialize matcher
    matcher = NDAANDAMatcher(main_table_df, orange_book_df)
    
    # Create matches
    matches = matcher.create_all_matches()
    logger.info(f"Created {len(matches)} Match objects")
    
    # Verify matches (using PDF validation by default)
    verified_matches = matcher.verify_all_matches(
        use_company_validation=True, 
        use_pdf_validation=use_pdf_validation
    )
    logger.info(f"Verified {len(verified_matches)} matches")
    
    # Get results
    results_df = matcher.get_results_dataframe()
    
    # Print summary statistics
    stats = matcher.get_summary_statistics()
    logger.info("=== MATCHING SUMMARY ===")
    for key, value in stats.items():
        if isinstance(value, float):
            logger.info(f"{key}: {value:.2f}")
        else:
            logger.info(f"{key}: {value}")
    
    logger.info("Class-based matching process complete!")
    
    return matcher, results_df


if __name__ == "__main__":
    # Example usage - would need actual data files
    logger.info("Class-based matching system ready for use")
    logger.info("Use run_class_based_matching(main_table_df, orange_book_df) to run the full process")