"""Data loading and cleaning utilities for the dosage analysis pipeline."""

from __future__ import annotations

import re
from datetime import datetime, timedelta
from typing import Tuple

import numpy as np
import pandas as pd
from pandas._libs.tslibs.nattype import NaTType


__all__ = [
    "preprocess_data",
    "load_workbooks",
    "clean_main_table",
    "clean_orange_book",
    "str_squish",
    "normalize_listish",
    "parse_main_date",
    "parse_ob_date",
]


def str_squish(value: object) -> str | float:
    """Collapse consecutive whitespace and trim the string representation."""
    if pd.isna(value):
        return value  # type: ignore[return-value]
    return re.sub(r"\s+", " ", str(value)).strip()


def normalize_listish(value: object) -> str | float:
    """Normalize list-like text representations (e.g. "['TABLET']") to a clean token."""
    if pd.isna(value):
        return np.nan
    cleaned = re.sub(r"[\[\]'\"]", "", str(value))
    return str_squish(cleaned).upper()


def parse_main_date(value: object) -> pd.Timestamp | NaTType:
    """Parse dates from the main dosage table."""
    try:
        return pd.to_datetime(value, errors="coerce")
    except Exception:
        return pd.NaT


def parse_ob_date(value: object) -> str | float:
    """Parse Orange Book approval dates using Excel serials or special strings."""
    if pd.isna(value):
        return np.nan

    # If it's already a datetime object, convert to string format
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.strftime("%Y-%m-%d")

    text = str(value)

    try:
        serial = float(text)
        converted = datetime(1899, 12, 30) + timedelta(days=int(serial))
        return converted.strftime("%Y-%m-%d")
    except Exception:
        pass

    if re.fullmatch(r"Approved Prior to Jan 1, 1982", text):
        return "Approved Prior to Jan 1, 1982"

    return np.nan


def load_workbooks(main_path: str, orange_path: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Load the raw Excel workbooks."""
    main_table = pd.read_excel(main_path)
    orange_book = pd.read_excel(orange_path)
    return main_table, orange_book


def clean_main_table(main_table: pd.DataFrame) -> pd.DataFrame:
    """Return a cleaned version of the main dosage table."""
    cleaned = pd.DataFrame(
        {
            "Appl_No": main_table["Appl_No"].astype(str),
            "Ingredient": main_table["API"].apply(
                lambda v: str_squish(v).upper() if not pd.isna(v) else np.nan
            ),
            "Approval_Date": main_table["Approval_Date"].apply(parse_main_date),
            "Product_Count": pd.to_numeric(main_table["Product_Count"], errors="coerce").astype("Int64"),
            "Strength_Count": pd.to_numeric(main_table["Strength_Count"], errors="coerce").astype("Int64"),
            "DF": main_table["DF"].apply(normalize_listish),
            "Route": main_table["Route"].apply(normalize_listish),
            "Strength": main_table["Strength"].apply(
                lambda v: str_squish(v).upper() if not pd.isna(v) else np.nan
            ),
            "MMT": pd.to_numeric(main_table["MMT"], errors="coerce"),
            "MMT_Years": pd.to_numeric(main_table["MMT_Years"], errors="coerce"),
        }
    )
    return cleaned


def clean_orange_book(orange_book: pd.DataFrame) -> pd.DataFrame:
    """Return a cleaned version of the Orange Book dataset."""
    df_route = orange_book["DF;Route"].astype(str).str.split(";", n=1, expand=True)

    cleaned = pd.DataFrame(
        {
            "Ingredient": orange_book["Ingredient"].apply(
                lambda v: str_squish(v).upper() if not pd.isna(v) else np.nan
            ),
            "DF": df_route[0].apply(normalize_listish),
            "Route": df_route[1].apply(normalize_listish) if 1 in df_route else np.nan,
            "Trade_Name": orange_book["Trade_Name"].apply(str_squish),
            "Applicant": orange_book["Applicant"].apply(str_squish),
            "Strength": orange_book["Strength"].apply(
                lambda v: str_squish(v).upper() if not pd.isna(v) else np.nan
            ),
            "Appl_Type": orange_book["Appl_Type"].apply(
                lambda v: str_squish(v).upper() if not pd.isna(v) else np.nan
            ),
            "Appl_No": orange_book["Appl_No"].astype(str),
            "Product_No": orange_book["Product_No"].astype(str),
            "TE_Code": orange_book["TE_Code"].apply(
                lambda v: str_squish(v).upper() if not pd.isna(v) else np.nan
            ),
            "Approval_Date": orange_book["Approval_Date"].apply(parse_ob_date),
            "RLD": orange_book["RLD"].apply(
                lambda v: str_squish(v).upper() if not pd.isna(v) else np.nan
            ),
            "RS": orange_book["RS"].apply(
                lambda v: str_squish(v).upper() if not pd.isna(v) else np.nan
            ),
            "type": orange_book["Type"].apply(
                lambda v: str_squish(v).upper() if not pd.isna(v) else np.nan
            ),
        }
    )
    return cleaned


def preprocess_data(main_path: str, orange_path: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Load and clean the main table and Orange Book datasets."""
    main_raw, orange_raw = load_workbooks(main_path, orange_path)
    main_clean = clean_main_table(main_raw)
    orange_clean = clean_orange_book(orange_raw)
    return main_clean, orange_clean
