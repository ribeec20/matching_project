"""Matching logic between NDA products and ANDA records."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import List

import numpy as np
import pandas as pd

from preprocess import str_squish


@dataclass
class MatchData:
    """Container for data frames produced during the matching stage."""

    study_ndas: pd.DataFrame
    study_ndas_strength: pd.DataFrame
    ndas_ob: pd.DataFrame
    andas_ob: pd.DataFrame
    study_ndas_final: pd.DataFrame
    candidates: pd.DataFrame
    anda_matches: pd.DataFrame
    nda_summary: pd.DataFrame
    ob_nda_first: pd.DataFrame
    date_check: pd.DataFrame


def norm_strength(value: object) -> str | float:
    """Normalize strength strings for comparison."""
    if pd.isna(value):
        return np.nan
    text = str(value).upper()
    text = text.replace(",", "")
    text = re.sub(r"[\[\]'\"]", "", text)
    text = re.sub(r"\s+", "", text)
    text = re.sub(r"MG\.?", "MG", text)
    text = text.replace("MCG", "MCG")
    text = text.replace("ML", "ML")
    return text


def tokenize_strength_list(value: object) -> List[str]:
    """Tokenize list-like strengths and normalize each entry."""
    if pd.isna(value):
        return []
    cleaned = re.sub(r"[\[\]'\"]", "", str(value))
    parts = re.split(r"\s*(\||;|,)+\s*", cleaned)
    tokens = [part for part in parts if part and part not in {"|", ";", ","}]
    return [norm_strength(token) for token in tokens if token.strip()]


def strength_in_tokens(tokens: List[str], normalized_strength: str | float) -> bool:
    if normalized_strength in (np.nan, None):
        return False
    return normalized_strength in (tokens or [])


def norm_tokens(value: object) -> List[str]:
    if pd.isna(value):
        return []
    text = str(value).upper()
    text = re.sub(r"[\[\]'\"]", "", text)
    text = re.sub(r"[^A-Z0-9]+", " ", text)
    text = str_squish(text)
    if not text:
        return []
    tokens = text.split(" ")
    seen: set[str] = set()
    ordered: List[str] = []
    for token in tokens:
        if token and token not in seen:
            seen.add(token)
            ordered.append(token)
    return ordered


def has_overlap(left: List[str], right: List[str]) -> bool:
    if not left or not right:
        return False
    return bool(set(left).intersection(right))


def te_compatible(val_a: object, val_b: object) -> bool:
    token_a = None if pd.isna(val_a) else str(val_a).upper()
    token_b = None if pd.isna(val_b) else str(val_b).upper()
    return (token_a is None) or (token_b is None) or (token_a == token_b)


def substr_contains(text: object, pattern: object) -> bool:
    if pd.isna(text) or pd.isna(pattern):
        return False
    return str(pattern) in str(text)


def coalesce_str(value_a: object, value_b: object) -> object:
    a = value_a if not (isinstance(value_a, float) and np.isnan(value_a)) else None
    b = value_b if not (isinstance(value_b, float) and np.isnan(value_b)) else None
    if isinstance(a, pd.Timestamp):
        a = a.strftime("%Y-%m-%d")
    if isinstance(b, pd.Timestamp):
        b = b.strftime("%Y-%m-%d")
    return a if a not in (None, "") else b


def shorter_than_granted(row: pd.Series) -> float | bool | np.bool_ | np.ndarray:
    actual_years = row["Actual_Monopoly_Years_Prod"]
    granted_years = row["NDA_MMT_Years"]
    if pd.isna(actual_years) or pd.isna(granted_years):
        return np.nan
    return bool(actual_years < granted_years)


def _extract_nda_and_anda_data(orange_book_clean: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Extract and prepare NDA and ANDA datasets from Orange Book."""
    # Extract NDAs (removing application type column as it's now redundant)
    ndas_ob = orange_book_clean.loc[orange_book_clean["Appl_Type"] == "N"].drop(
        columns=["Appl_Type"], errors="ignore"
    )
    
    # Extract ANDAs and prefix all columns to avoid naming conflicts
    andas_ob = orange_book_clean.loc[orange_book_clean["Appl_Type"] == "A"].copy()
    andas_ob.columns = [f"ANDA_{col}" for col in andas_ob.columns]
    if "ANDA_Appl_Type" in andas_ob.columns:
        andas_ob = andas_ob.drop(columns=["ANDA_Appl_Type"])
    
    return ndas_ob, andas_ob


def _merge_study_ndas_with_orange_book(
    main_table_clean: pd.DataFrame, ndas_ob: pd.DataFrame
) -> pd.DataFrame:
    """Merge study NDAs with Orange Book NDA data."""
    return main_table_clean.merge(
        ndas_ob, how="left", on="Appl_No", suffixes=("", "_nda")
    )


def _process_strength_matching(study_ndas: pd.DataFrame) -> pd.DataFrame:
    """Process strength data for flexible strength matching."""
    study_ndas_strength = study_ndas.copy()
    study_ndas_strength["strength_x_raw"] = study_ndas_strength["Strength"]
    study_ndas_strength["strength_y_raw"] = study_ndas_strength.get("Strength_nda")
    
    # Tokenize strength lists for multi-strength products
    study_ndas_strength["strength_x_tokens"] = study_ndas_strength["strength_x_raw"].apply(
        tokenize_strength_list
    )
    study_ndas_strength["strength_y_norm"] = study_ndas_strength["strength_y_raw"].apply(
        norm_strength
    )
    
    # Check if normalized strength appears in tokenized list
    study_ndas_strength["strength_y_in_tokens"] = study_ndas_strength.apply(
        lambda row: strength_in_tokens(row["strength_x_tokens"], row["strength_y_norm"]),
        axis=1,
    )
    
    # Normalize both strengths for direct comparison
    study_ndas_strength["strength_x_norm"] = study_ndas_strength["strength_x_raw"].apply(
        norm_strength
    )
    study_ndas_strength["strength_y_in_substr"] = study_ndas_strength.apply(
        lambda row: substr_contains(row["strength_x_norm"], row["strength_y_norm"]),
        axis=1,
    )
    
    # Final strength match combines both approaches
    study_ndas_strength["strength_match"] = (
        study_ndas_strength["strength_y_in_tokens"]
        | study_ndas_strength["strength_y_in_substr"]
    )
    
    return study_ndas_strength


def _process_date_validation(study_ndas: pd.DataFrame, ndas_ob: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Process and validate approval dates between datasets."""
    ndas_ob_dates = ndas_ob.copy()
    ndas_ob_dates["_Approval_Date_dt"] = pd.to_datetime(
        ndas_ob_dates["Approval_Date"], errors="coerce"
    )
    
    # Get earliest approval date per NDA
    ob_nda_first = (
        ndas_ob_dates.groupby("Appl_No", dropna=False)["_Approval_Date_dt"]
        .min()
        .rename("OB_NDA_First_Approval")
        .reset_index()
    )

    # Compare dates between main table and Orange Book
    date_check = (
        study_ndas[["Appl_No", "Approval_Date"]]
        .drop_duplicates()
        .merge(ob_nda_first, how="left", on="Appl_No")
        .rename(columns={"Approval_Date": "Approval_Date_x"})
    )
    date_check["main_date"] = pd.to_datetime(date_check["Approval_Date_x"], errors="coerce")
    date_check["ob_date"] = date_check["OB_NDA_First_Approval"]
    date_check["both_non_na"] = date_check["main_date"].notna() & date_check["ob_date"].notna()
    date_check["date_equal"] = (
        date_check["both_non_na"]
        & (date_check["main_date"] == date_check["ob_date"])
    )
    date_check["date_diff_days"] = np.where(
        date_check["both_non_na"],
        (date_check["ob_date"] - date_check["main_date"]).dt.days.astype("float"),
        np.nan,
    )
    
    return ob_nda_first, date_check


def _consolidate_study_nda_data(study_ndas_strength: pd.DataFrame) -> pd.DataFrame:
    """Consolidate and clean study NDA data with proper column naming."""
    sdf = study_ndas_strength.copy()
    
    # Coalesce data from main table and Orange Book (prioritizing main table)
    sdf["Ingredient"] = sdf.apply(
        lambda row: coalesce_str(row.get("Ingredient"), row.get("Ingredient_nda")),
        axis=1,
    )
    sdf["Approval_Date"] = sdf.apply(
        lambda row: coalesce_str(row.get("Approval_Date"), row.get("Approval_Date_nda")),
        axis=1,
    )
    sdf["DF"] = sdf.apply(
        lambda row: coalesce_str(row.get("DF"), row.get("DF_nda")), axis=1
    )
    sdf["Route"] = sdf.apply(
        lambda row: coalesce_str(row.get("Route"), row.get("Route_nda")), axis=1
    )
    sdf = sdf.rename(
        columns={
            "Strength": "Strength_List",
            "Strength_nda": "Strength_Specific",
        }
    )

    # Reorganize columns for clarity
    columns_front = [
        "Appl_No",
        "Ingredient",
        "Approval_Date",
        "DF",
        "Route",
        "Product_Count",
        "Strength_Count",
        "Strength_List",
        "Strength_Specific",
        "MMT",
        "MMT_Years",
    ]
    remaining_cols = [col for col in sdf.columns if col not in columns_front]
    study_ndas_final = sdf[columns_front + remaining_cols].copy()

    # Clean up temporary processing columns
    drop_cols = [
        col
        for col in study_ndas_final.columns
        if col.endswith(("_x", "_y", "_nda"))
        or col
        in {
            "strength_x_tokens",
            "strength_y_norm",
            "strength_x_norm",
            "strength_y_in_tokens",
            "strength_y_in_substr",
            "strength_match",
            "strength_x_raw",
            "strength_y_raw",
        }
    ]
    drop_cols = [col for col in drop_cols if col in study_ndas_final.columns]
    study_ndas_final = study_ndas_final.drop(columns=drop_cols)
    
    # Prefix all columns with NDA_ for clarity
    study_ndas_final.columns = [f"NDA_{col}" for col in study_ndas_final.columns]
    
    return study_ndas_final


def _prepare_matching_datasets(study_ndas_final: pd.DataFrame, andas_ob: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Prepare NDA and ANDA datasets for matching with normalized fields."""
    # Prepare NDA data for matching
    nda_prod = study_ndas_final.copy()
    nda_prod["NDA_ING_KEY"] = nda_prod["NDA_Ingredient"].apply(
        lambda value: str_squish(value).upper() if not pd.isna(value) else np.nan
    )
    nda_prod["NDA_DF_TOK"] = nda_prod["NDA_DF"].apply(norm_tokens)
    nda_prod["NDA_RT_TOK"] = nda_prod["NDA_Route"].apply(norm_tokens)
    nda_prod["NDA_STR_N"] = nda_prod["NDA_Strength_Specific"].apply(norm_strength)

    # Prepare ANDA data for matching
    andas_prep = andas_ob.copy()
    andas_prep["ANDA_ING_KEY"] = andas_prep["ANDA_Ingredient"].apply(
        lambda value: str_squish(value).upper() if not pd.isna(value) else np.nan
    )
    andas_prep["ANDA_DF_TOK"] = andas_prep["ANDA_DF"].apply(norm_tokens)
    andas_prep["ANDA_RT_TOK"] = andas_prep["ANDA_Route"].apply(norm_tokens)
    andas_prep["ANDA_STR_N"] = andas_prep["ANDA_Strength"].apply(norm_strength)
    andas_prep["ANDA_Approval_Date_Date"] = pd.to_datetime(
        andas_prep["ANDA_Approval_Date"], errors="coerce"
    )
    
    return nda_prod, andas_prep


def _perform_ingredient_based_matching(nda_prod: pd.DataFrame, andas_prep: pd.DataFrame) -> pd.DataFrame:
    """Perform initial ingredient-based matching to create candidate pairs."""
    return nda_prod.merge(
        andas_prep,
        how="inner",
        left_on="NDA_ING_KEY",
        right_on="ANDA_ING_KEY",
    )


def _apply_matching_criteria(candidates: pd.DataFrame) -> pd.DataFrame:
    """Apply the 3 matching criteria: DF_OK, RT_OK, STR_OK."""
    # DF_OK: Dosage forms must have overlapping tokens
    candidates["DF_OK"] = candidates.apply(
        lambda row: has_overlap(row.get("NDA_DF_TOK"), row.get("ANDA_DF_TOK")),
        axis=1,
    )
    
    # RT_OK: Routes must have overlapping tokens  
    candidates["RT_OK"] = candidates.apply(
        lambda row: has_overlap(row.get("NDA_RT_TOK"), row.get("ANDA_RT_TOK")),
        axis=1,
    )
    
    # STR_OK: Strengths must match exactly after normalization
    candidates["STR_OK"] = candidates["NDA_STR_N"] == candidates["ANDA_STR_N"]
    
    return candidates


def _filter_final_matches(candidates: pd.DataFrame) -> pd.DataFrame:
    """Filter candidates to final matches based on the 3 essential criteria."""
    return candidates.query("DF_OK and RT_OK and STR_OK").copy()


def _create_nda_summary(study_ndas_final: pd.DataFrame) -> pd.DataFrame:
    """Create basic NDA summary for further processing."""
    nda_summary = (
        study_ndas_final[
            [
                "NDA_Appl_No",
                "NDA_Approval_Date",
                "NDA_MMT_Years",
                "NDA_Ingredient",
                "NDA_Applicant",
            ]
        ]
        .drop_duplicates(subset=["NDA_Appl_No"])
        .copy()
    )
    nda_summary["NDA_Approval_Date_Date"] = pd.to_datetime(
        nda_summary["NDA_Approval_Date"], errors="coerce"
    )
    return nda_summary


def match_ndas_to_andas(
    main_table_clean: pd.DataFrame, orange_book_clean: pd.DataFrame
) -> MatchData:
    """
    Run the NDA-to-ANDA matching algorithm using 3 criteria:
    - DF_OK: Dosage forms overlap (e.g., both contain "TABLET")
    - RT_OK: Routes overlap (e.g., both contain "ORAL") 
    - STR_OK: Strengths match exactly (e.g., both "10MG")
    """
    
    # Step 1: Extract and prepare base datasets
    ndas_ob, andas_ob = _extract_nda_and_anda_data(orange_book_clean)
    
    # Step 2: Merge study NDAs with Orange Book data
    study_ndas = _merge_study_ndas_with_orange_book(main_table_clean, ndas_ob)

    # Step 3: Process strength matching logic
    study_ndas_strength = _process_strength_matching(study_ndas)
    
    # Step 4: Process and validate approval dates
    ob_nda_first, date_check = _process_date_validation(study_ndas, ndas_ob)
    
    # Step 5: Consolidate and clean study NDA data
    study_ndas_final = _consolidate_study_nda_data(study_ndas_strength)
    
    # Step 6: Validate required columns exist
    assert "NDA_Product_No" in study_ndas_final.columns, "NDA_Product_No missing"
    assert all(
        key in andas_ob.columns
        for key in ["ANDA_Appl_No", "ANDA_Product_No", "ANDA_Approval_Date"]
    ), "ANDA key columns missing"
    
    # Step 7: Prepare datasets for matching with normalized fields
    nda_prod, andas_prep = _prepare_matching_datasets(study_ndas_final, andas_ob)
    
    # Step 8: Perform ingredient-based matching to create candidates
    candidates = _perform_ingredient_based_matching(nda_prod, andas_prep)
    
    # Step 9: Apply the 3 matching criteria
    candidates = _apply_matching_criteria(candidates)
    
    # Step 10: Filter to final matches (only DF_OK, RT_OK, STR_OK - removed TE_OK)
    anda_matches = _filter_final_matches(candidates)
    
    # Step 11: Create NDA summary for downstream processing
    nda_summary = _create_nda_summary(study_ndas_final)

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
