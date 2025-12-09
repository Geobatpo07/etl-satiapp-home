"""
Extractor module for ETL pipeline.

This module handles loading the CSV file containing feedback data.
"""

import pandas as pd
from pathlib import Path
from config.settings import CSV_PATH, ENCODING_FIXES


def load_csv(csv_path: Path = CSV_PATH) -> pd.DataFrame:
    """
    Load CSV file and return a pandas DataFrame.
    
    This function:
    - Loads the CSV file from the specified path
    - Handles encoding issues automatically
    - Validates that the file exists
    - Returns all columns from the source file
    
    Args:
        csv_path: Path to the CSV file (defaults to settings.CSV_PATH)
    
    Returns:
        pd.DataFrame: Raw DataFrame with all columns from CSV
    
    Raises:
        FileNotFoundError: If the CSV file doesn't exist
        pd.errors.EmptyDataError: If the CSV file is empty
        Exception: For other CSV reading errors
    
    Example:
        >>> df = load_csv()
        >>> print(f"Loaded {len(df)} rows and {len(df.columns)} columns")
    """
    print(f"📂 Loading CSV file: {csv_path}")
    
    # Validate file exists
    if not Path(csv_path).exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")
    
    try:
        # Try loading with UTF-8 encoding first
        df = pd.read_csv(csv_path, encoding='utf-8')
        print(f"✅ Successfully loaded CSV with UTF-8 encoding")
    except UnicodeDecodeError:
        try:
            # Fallback to latin1 encoding
            df = pd.read_csv(csv_path, encoding='latin1')
            print(f"✅ Successfully loaded CSV with latin1 encoding")
        except Exception as e:
            # Try with cp1252 (Windows encoding)
            try:
                df = pd.read_csv(csv_path, encoding='cp1252')
                print(f"✅ Successfully loaded CSV with cp1252 encoding")
            except Exception as e:
                raise Exception(f"Failed to load CSV with any encoding: {e}")
    
    # Apply encoding fixes to all string columns
    for col in df.columns:
        if df[col].dtype == 'object':  # String columns
            for wrong, correct in ENCODING_FIXES.items():
                df[col] = df[col].astype(str).str.replace(wrong, correct, regex=False)
    
    print(f"📊 Loaded {len(df)} rows and {len(df.columns)} columns")
    print(f"📋 Columns: {', '.join(df.columns[:5])}{'...' if len(df.columns) > 5 else ''}")
    
    return df


def preview_data(df: pd.DataFrame, n_rows: int = 5) -> None:
    """
    Print a preview of the DataFrame.
    
    Args:
        df: DataFrame to preview
        n_rows: Number of rows to display (default: 5)
    """
    print(f"\n📋 Data Preview (first {n_rows} rows):")
    print(df.head(n_rows))
    print(f"\n📊 Data Info:")
    print(f"  - Shape: {df.shape}")
    print(f"  - Memory usage: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
    print(f"  - Missing values: {df.isnull().sum().sum()}")


if __name__ == "__main__":
    # Test the extractor
    df = load_csv()
    preview_data(df)
