"""Data loading and cleaning utilities for NDA/ANDA class instantiation."""

from __future__ import annotations

import logging
from typing import Dict, List, Tuple

import pandas as pd

from match_class import NDA, ANDA
from preprocess import str_squish

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataLoader:
    """Class to handle data loading and cleaning for NDA/ANDA instantiation."""
    
    def __init__(self, main_table_path: str = None, orange_book_path: str = None):
        """Initialize DataLoader with optional file paths.
        
        Args:
            main_table_path: Path to main table Excel file
            orange_book_path: Path to Orange Book Excel file
        """
        self.main_table_path = main_table_path
        self.orange_book_path = orange_book_path
        self.main_table_clean = None
        self.orange_book_clean = None
        self.nda_objects = {}
        self.anda_objects = {}
        
    def load_and_clean_data(self, main_table_df: pd.DataFrame = None, 
                           orange_book_df: pd.DataFrame = None) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Load and clean main table and Orange Book data.
        
        Args:
            main_table_df: Optional pre-loaded main table DataFrame
            orange_book_df: Optional pre-loaded Orange Book DataFrame
            
        Returns:
            Tuple of (cleaned_main_table, cleaned_orange_book)
        """
        logger.info("Starting data loading and cleaning process...")
        
        # Load data if not provided
        if main_table_df is not None:
            self.main_table_clean = main_table_df.copy()
            logger.info(f"Using provided main table DataFrame with {len(self.main_table_clean)} rows")
        elif self.main_table_path:
            logger.info(f"Loading main table from {self.main_table_path}")
            self.main_table_clean = pd.read_excel(self.main_table_path)
            logger.info(f"Loaded main table with {len(self.main_table_clean)} rows")
        else:
            raise ValueError("Either main_table_df or main_table_path must be provided")
        
        if orange_book_df is not None:
            self.orange_book_clean = orange_book_df.copy()
            logger.info(f"Using provided Orange Book DataFrame with {len(self.orange_book_clean)} rows")
        elif self.orange_book_path:
            logger.info(f"Loading Orange Book from {self.orange_book_path}")
            self.orange_book_clean = pd.read_excel(self.orange_book_path)
            logger.info(f"Loaded Orange Book with {len(self.orange_book_clean)} rows")
        else:
            raise ValueError("Either orange_book_df or orange_book_path must be provided")
        
        # Clean the data
        self._clean_main_table()
        self._clean_orange_book()
        
        return self.main_table_clean, self.orange_book_clean
    
    def _clean_main_table(self) -> None:
        """Clean and standardize main table data."""
        logger.info("Cleaning main table data...")
        
        # Ensure Appl_No is string
        self.main_table_clean['Appl_No'] = self.main_table_clean['Appl_No'].astype(str)
        
        # Clean string columns
        string_columns = ['Ingredient', 'Applicant', 'DF', 'Route', 'MMT']
        for col in string_columns:
            if col in self.main_table_clean.columns:
                self.main_table_clean[col] = self.main_table_clean[col].apply(
                    lambda x: str_squish(str(x)) if pd.notna(x) else ''
                )
        
        # Clean numeric columns
        numeric_columns = ['Product_Count', 'Strength_Count', 'MMT_Years']
        for col in numeric_columns:
            if col in self.main_table_clean.columns:
                self.main_table_clean[col] = pd.to_numeric(self.main_table_clean[col], errors='coerce')
        
        # Clean date columns
        if 'Approval_Date' in self.main_table_clean.columns:
            self.main_table_clean['Approval_Date'] = pd.to_datetime(
                self.main_table_clean['Approval_Date'], errors='coerce'
            )
        
        logger.info(f"Main table cleaned: {len(self.main_table_clean)} rows")
    
    def _clean_orange_book(self) -> None:
        """Clean and standardize Orange Book data.""" 
        logger.info("Cleaning Orange Book data...")
        
        # Ensure Appl_No is string
        self.orange_book_clean['Appl_No'] = self.orange_book_clean['Appl_No'].astype(str)
        
        # Clean string columns
        string_columns = [
            'Ingredient', 'Applicant', 'DF', 'Route', 'Trade_Name', 
            'TE_Code', 'RLD', 'RS', 'Type', 'Marketing_Status'
        ]
        for col in string_columns:
            if col in self.orange_book_clean.columns:
                self.orange_book_clean[col] = self.orange_book_clean[col].apply(
                    lambda x: str_squish(str(x)) if pd.notna(x) else ''
                )
        
        # Clean Product_No
        if 'Product_No' in self.orange_book_clean.columns:
            self.orange_book_clean['Product_No'] = self.orange_book_clean['Product_No'].astype(str)
        
        # Clean date columns
        if 'Approval_Date' in self.orange_book_clean.columns:
            self.orange_book_clean['Approval_Date'] = pd.to_datetime(
                self.orange_book_clean['Approval_Date'], errors='coerce'
            )
        
        logger.info(f"Orange Book cleaned: {len(self.orange_book_clean)} rows")
    
    def create_nda_objects(self) -> Dict[str, NDA]:
        """Create NDA objects for each NDA in the main table.
        
        Returns:
            Dictionary mapping NDA number to NDA object
        """
        logger.info("Creating NDA objects...")
        
        if self.main_table_clean is None or self.orange_book_clean is None:
            raise ValueError("Data must be loaded and cleaned before creating NDA objects")
        
        self.nda_objects = {}
        
        for _, main_row in self.main_table_clean.iterrows():
            nda_number = str(main_row['Appl_No'])
            
            # Get corresponding Orange Book rows for this NDA
            ob_rows = self.orange_book_clean[
                (self.orange_book_clean['Appl_No'] == nda_number) & 
                (self.orange_book_clean['Appl_Type'] == 'N')
            ].copy()
            
            # Create NDA object
            nda = NDA(main_row, ob_rows)
            self.nda_objects[nda_number] = nda
            
            logger.debug(f"Created NDA object for {nda_number}: {nda.get_ingredient()}")
        
        logger.info(f"Created {len(self.nda_objects)} NDA objects")
        return self.nda_objects
    
    def create_anda_objects(self) -> Dict[str, ANDA]:
        """Create ANDA objects for each ANDA in the Orange Book.
        
        Returns:
            Dictionary mapping ANDA number to ANDA object
        """
        logger.info("Creating ANDA objects...")
        
        if self.orange_book_clean is None:
            raise ValueError("Orange Book data must be loaded and cleaned before creating ANDA objects")
        
        self.anda_objects = {}
        
        # Filter for ANDA records only
        anda_records = self.orange_book_clean[
            self.orange_book_clean['Appl_Type'] == 'A'
        ].copy()
        
        for _, anda_row in anda_records.iterrows():
            anda_number = str(anda_row['Appl_No'])
            
            # Create ANDA object
            anda = ANDA(anda_row)
            self.anda_objects[anda_number] = anda
            
            logger.debug(f"Created ANDA object for {anda_number}: {anda.get_ingredient()}")
        
        logger.info(f"Created {len(self.anda_objects)} ANDA objects")
        return self.anda_objects
    
    def get_nda_by_number(self, nda_number: str) -> NDA:
        """Get NDA object by number.
        
        Args:
            nda_number: NDA application number
            
        Returns:
            NDA object
            
        Raises:
            KeyError: If NDA number not found
        """
        if not self.nda_objects:
            raise ValueError("NDA objects have not been created yet")
        
        return self.nda_objects[str(nda_number)]
    
    def get_anda_by_number(self, anda_number: str) -> ANDA:
        """Get ANDA object by number.
        
        Args:
            anda_number: ANDA application number
            
        Returns:
            ANDA object
            
        Raises:
            KeyError: If ANDA number not found
        """
        if not self.anda_objects:
            raise ValueError("ANDA objects have not been created yet")
        
        return self.anda_objects[str(anda_number)]
    
    def get_andas_by_ingredient(self, ingredient: str) -> List[ANDA]:
        """Get all ANDA objects for a specific ingredient.
        
        Args:
            ingredient: Active ingredient name
            
        Returns:
            List of ANDA objects matching the ingredient
        """
        if not self.anda_objects:
            raise ValueError("ANDA objects have not been created yet")
        
        normalized_ingredient = str_squish(ingredient).upper()
        matching_andas = []
        
        for anda in self.anda_objects.values():
            if anda.get_normalized_ingredient() == normalized_ingredient:
                matching_andas.append(anda)
        
        return matching_andas
    
    def get_ndas_by_ingredient(self, ingredient: str) -> List[NDA]:
        """Get all NDA objects for a specific ingredient.
        
        Args:
            ingredient: Active ingredient name
            
        Returns:
            List of NDA objects matching the ingredient
        """
        if not self.nda_objects:
            raise ValueError("NDA objects have not been created yet")
        
        normalized_ingredient = str_squish(ingredient).upper()
        matching_ndas = []
        
        for nda in self.nda_objects.values():
            if nda.get_normalized_ingredient() == normalized_ingredient:
                matching_ndas.append(nda)
        
        return matching_ndas
    
    def get_all_ndas(self) -> List[NDA]:
        """Get all NDA objects.
        
        Returns:
            List of all NDA objects
        """
        if not self.nda_objects:
            raise ValueError("NDA objects have not been created yet")
        
        return list(self.nda_objects.values())
    
    def get_all_andas(self) -> List[ANDA]:
        """Get all ANDA objects.
        
        Returns:
            List of all ANDA objects
        """
        if not self.anda_objects:
            raise ValueError("ANDA objects have not been created yet")
        
        return list(self.anda_objects.values())
    
    def validate_data_integrity(self) -> Dict[str, int]:
        """Validate data integrity and return summary statistics.
        
        Returns:
            Dictionary with validation statistics
        """
        stats = {
            'main_table_rows': len(self.main_table_clean) if self.main_table_clean is not None else 0,
            'orange_book_rows': len(self.orange_book_clean) if self.orange_book_clean is not None else 0,
            'nda_objects_created': len(self.nda_objects),
            'anda_objects_created': len(self.anda_objects),
        }
        
        if self.orange_book_clean is not None:
            stats['orange_book_ndas'] = len(
                self.orange_book_clean[self.orange_book_clean['Appl_Type'] == 'N']
            )
            stats['orange_book_andas'] = len(
                self.orange_book_clean[self.orange_book_clean['Appl_Type'] == 'A']
            )
        
        # Validate that all main table NDAs have objects
        if self.main_table_clean is not None and self.nda_objects:
            main_table_ndas = set(self.main_table_clean['Appl_No'].astype(str))
            nda_object_numbers = set(self.nda_objects.keys())
            stats['missing_nda_objects'] = len(main_table_ndas - nda_object_numbers)
            stats['extra_nda_objects'] = len(nda_object_numbers - main_table_ndas)
        
        logger.info(f"Data validation stats: {stats}")
        return stats


def load_and_create_objects(main_table_df: pd.DataFrame, 
                          orange_book_df: pd.DataFrame) -> Tuple[Dict[str, NDA], Dict[str, ANDA], DataLoader]:
    """Convenience function to load data and create all objects.
    
    Args:
        main_table_df: Main table DataFrame
        orange_book_df: Orange Book DataFrame
        
    Returns:
        Tuple of (nda_objects_dict, anda_objects_dict, data_loader)
    """
    loader = DataLoader()
    loader.load_and_clean_data(main_table_df, orange_book_df)
    nda_objects = loader.create_nda_objects()
    anda_objects = loader.create_anda_objects()
    
    # Validate data integrity
    stats = loader.validate_data_integrity()
    logger.info(f"Data loading complete. Created {len(nda_objects)} NDAs and {len(anda_objects)} ANDAs")
    
    return nda_objects, anda_objects, loader