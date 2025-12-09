"""
Validator Module for ETL Pipeline.

This module is responsible for:
1. Saving transformed data to a staging SQLite database.
2. Validating the schema of the data before loading.
"""

import sqlite3
import pandas as pd
from typing import Tuple, List
from config.settings import STAGING_DB_PATH, EXPECTED_COLUMNS

def save_to_staging(df: pd.DataFrame, table_name: str = "staging_data") -> None:
    """
    Save the DataFrame to a SQLite staging database.
    
    Args:
        df (pd.DataFrame): The DataFrame to save.
        table_name (str): Name of the table in SQLite.
    """
    try:
        # Create directory if it doesn't exist
        STAGING_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        
        # Connect to SQLite
        with sqlite3.connect(STAGING_DB_PATH) as conn:
            # Write to database (replace if exists)
            df.to_sql(table_name, conn, if_exists="replace", index=False)
            print(f"  Data saved to staging database: {STAGING_DB_PATH}")
            print(f"  Table name: {table_name}")
            
    except Exception as e:
        print(f"  Error saving to staging: {e}")
        raise

def validate_columns(df: pd.DataFrame) -> Tuple[bool, List[str]]:
    """
    Validate that the DataFrame columns match the expected columns.
    
    Args:
        df (pd.DataFrame): The DataFrame to validate.
        
    Returns:
        Tuple[bool, List[str]]: (isValid, list of error messages)
    """
    current_columns = list(df.columns)
    errors = []
    
    # Check for missing columns
    missing_columns = [col for col in EXPECTED_COLUMNS if col not in current_columns]
    if missing_columns:
        errors.append(f"Missing columns: {missing_columns}")
        
    # Check for extra columns (optional, but good for strict control)
    extra_columns = [col for col in current_columns if col not in EXPECTED_COLUMNS]
    if extra_columns:
        errors.append(f"Unexpected columns found: {extra_columns}")
        
    # Check order: do not fail the validation just for order; we'll reorder later
    if current_columns != EXPECTED_COLUMNS:
        if not missing_columns and not extra_columns:
            # Only warn about order when the sets match
            print("  Column order differs from expected schema (will be reordered).")

    if errors:
        return False, errors
    
    return True, []


def reorder_columns(
    df: pd.DataFrame,
    expected_columns: List[str] = None,
    keep_extra: bool = True,
) -> pd.DataFrame:
    """
    Reorder DataFrame columns to match the expected schema.

    Args:
        df: Input DataFrame
        expected_columns: Target column order (defaults to settings.EXPECTED_COLUMNS)
        keep_extra: If True, keeps any extra columns after the expected ones

    Returns:
        pd.DataFrame: DataFrame with columns reordered
    """
    expected = expected_columns or EXPECTED_COLUMNS
    # Preserve columns that exist in df and are in expected (in that order)
    aligned = [c for c in expected if c in df.columns]
    # Collect extra columns that are not in expected (in their current order)
    extras = [c for c in df.columns if c not in expected]

    new_order = aligned + (extras if keep_extra else [])
    return df[new_order]
