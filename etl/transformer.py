"""
Transformer module for ETL pipeline.

This module applies all transformations to the DataFrame, including:
1. VBA transformations (column movements, deletions, renaming)
2. Power Query transformations (combining columns, filtering, mapping ratings)
"""

import pandas as pd
import numpy as np
from typing import List
from config.settings import RATING_MAPPINGS, TEXT_STANDARDIZATION, COLUMNS_TO_REMOVE


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply all transformations to the DataFrame.
    
    This function replicates:
    1. All VBA transformations from PreparationFeedback() macro
    2. All Power Query transformations
    
    Args:
        df: Raw DataFrame from CSV
    
    Returns:
        pd.DataFrame: Transformed DataFrame ready for Access database
    
    Example:
        >>> df_raw = load_csv()
        >>> df_clean = transform(df_raw)
    """
    print("\n🔄 Starting transformations...")
    
    # Make a copy to avoid modifying the original
    df = df.copy()
    
    # ========================================================================
    # PART 1: VBA TRANSFORMATIONS
    # ========================================================================
    print("  📝 Applying VBA transformations...")
    df = apply_vba_transformations(df)
    
    # ========================================================================
    # PART 2: POWER QUERY TRANSFORMATIONS
    # ========================================================================
    print("  📝 Applying Power Query transformations...")
    df = apply_power_query_transformations(df)
    
    print(f"✅ Transformations complete! Final shape: {df.shape}")
    return df


def apply_vba_transformations(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply VBA transformations from PreparationFeedback() macro.
    
    Transformations:
    1. Delete column AC (if exists)
    2. Move block V:AA to the end
    3. Move block AJ:AR to the end
    4. Remove empty columns in moved blocks
    5. Insert column AV after AU
    6. Move column V to AV
    7. Rename columns AH, AO, AT
    
    Args:
        df: DataFrame to transform
    
    Returns:
        pd.DataFrame: Transformed DataFrame
    """
    # Note: Since we don't know exact column names, we'll work with column indices
    # This is a template - you may need to adjust based on actual CSV structure
    
    # 1. Delete column AC (index 28, 0-based)
    # Only delete if the column exists
    if len(df.columns) > 28:
        col_ac = df.columns[28]
        print(f"    - Deleting column AC: {col_ac}")
        df = df.drop(columns=[col_ac])
    
    # 2. Move blocks V:AA (columns 21-26) to the end
    # Get column names for the block
    if len(df.columns) > 26:
        block_v_aa_start = 21  # Column V (0-based index)
        block_v_aa_end = 27    # Column AA (inclusive)
        
        if len(df.columns) >= block_v_aa_end:
            block_v_aa_cols = df.columns[block_v_aa_start:block_v_aa_end].tolist()
            print(f"    - Moving block V:AA ({len(block_v_aa_cols)} columns) to end")
            
            # Reorder: keep everything except the block, then add the block at the end
            other_cols = [col for col in df.columns if col not in block_v_aa_cols]
            df = df[other_cols + block_v_aa_cols]
    
    # 3. Move block AJ:AR (columns 35-43) to the end
    # Note: indices may have shifted after previous operations
    # We'll use column names if we know them, or indices
    if len(df.columns) > 43:
        # Adjust indices based on previous deletions
        block_aj_ar_start = 35
        block_aj_ar_end = 44
        
        if len(df.columns) >= block_aj_ar_end:
            block_aj_ar_cols = df.columns[block_aj_ar_start:block_aj_ar_end].tolist()
            print(f"    - Moving block AJ:AR ({len(block_aj_ar_cols)} columns) to end")
            
            other_cols = [col for col in df.columns if col not in block_aj_ar_cols]
            df = df[other_cols + block_aj_ar_cols]
    
    # 4. Remove empty columns in moved blocks
    # Identify columns that are completely empty (all NaN or empty strings)
    empty_cols = []
    for col in df.columns:
        if df[col].isna().all() or (df[col].astype(str).str.strip() == '').all():
            empty_cols.append(col)
    
    if empty_cols:
        print(f"    - Removing {len(empty_cols)} empty columns")
        df = df.drop(columns=empty_cols)
    
    # 5. Insert column AV after AU
    # This creates a new empty column
    if 'AU' in df.columns:
        au_index = df.columns.get_loc('AU')
        print(f"    - Inserting column AV after AU")
        df.insert(au_index + 1, 'AV', np.nan)
    
    # 6. Move column V to AV
    if 'V' in df.columns and 'AV' in df.columns:
        print(f"    - Moving data from column V to AV")
        df['AV'] = df['V']
        df = df.drop(columns=['V'])
    
    # 7. Rename columns AH, AO, AT
    # You'll need to specify what these should be renamed to
    # For now, we'll add a suffix to demonstrate
    rename_map = {}
    if 'AH' in df.columns:
        rename_map['AH'] = 'AH_Renamed'
    if 'AO' in df.columns:
        rename_map['AO'] = 'AO_Renamed'
    if 'AT' in df.columns:
        rename_map['AT'] = 'AT_Renamed'
    
    if rename_map:
        print(f"    - Renaming columns: {', '.join(rename_map.keys())}")
        df = df.rename(columns=rename_map)
    
    return df


def apply_power_query_transformations(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply Power Query transformations.
    
    Transformations:
    1. Promote headers (already done by pandas read_csv)
    2. Combine columns for hospital questions (10 columns)
    3. Combine columns for satisfaction (5 columns)
    4. Combine columns for dissatisfaction (5 columns)
    5. Combine columns for mistreatment
    6. Filter Respondent ID
    7. Remove unwanted columns
    8. Map ratings to stars
    9. Standardize text values
    
    Args:
        df: DataFrame to transform
    
    Returns:
        pd.DataFrame: Transformed DataFrame
    """
    # 1. Promote headers - already done by pandas
    
    # 2. Combine columns for "Nan ki lopital..." (10 columns)
    # Find columns that match this pattern
    hospital_cols = [col for col in df.columns if 'lopital' in col.lower() or 'Nan ki lopital' in col]
    if hospital_cols:
        print(f"    - Combining {len(hospital_cols)} hospital columns")
        df['Hospital_Combined'] = df[hospital_cols].apply(
            lambda row: '; '.join(row.dropna().astype(str)), axis=1
        )
    
    # 3. Combine columns for "De kisa ou satisfè..." (5 columns)
    satisfaction_cols = [col for col in df.columns if 'satisfè' in col.lower() or 'satisfe' in col.lower()]
    if satisfaction_cols:
        print(f"    - Combining {len(satisfaction_cols)} satisfaction columns")
        df['Satisfaction_Combined'] = df[satisfaction_cols].apply(
            lambda row: '; '.join(row.dropna().astype(str)), axis=1
        )
    
    # 4. Combine columns for "Ki pèsonèl ou pa satisfè..." (5 columns)
    dissatisfaction_cols = [col for col in df.columns if 'pa satisfè' in col.lower() or 'pa satisfe' in col.lower()]
    if dissatisfaction_cols:
        print(f"    - Combining {len(dissatisfaction_cols)} dissatisfaction columns")
        df['Dissatisfaction_Combined'] = df[dissatisfaction_cols].apply(
            lambda row: '; '.join(row.dropna().astype(str)), axis=1
        )
    
    # 5. Combine columns for "Ki moun ki mal gade w..."
    mistreatment_cols = [col for col in df.columns if 'mal gade' in col.lower()]
    if mistreatment_cols:
        print(f"    - Combining {len(mistreatment_cols)} mistreatment columns")
        df['Mistreatment_Combined'] = df[mistreatment_cols].apply(
            lambda row: '; '.join(row.dropna().astype(str)), axis=1
        )
    
    # 6. Filter Respondent ID (remove rows with empty Respondent ID)
    if 'Respondent ID' in df.columns:
        initial_rows = len(df)
        df = df[df['Respondent ID'].notna()]
        df = df[df['Respondent ID'].astype(str).str.strip() != '']
        removed_rows = initial_rows - len(df)
        if removed_rows > 0:
            print(f"    - Filtered out {removed_rows} rows with empty Respondent ID")
    
    # 7. Remove unwanted columns
    cols_to_remove = [col for col in COLUMNS_TO_REMOVE if col in df.columns]
    if cols_to_remove:
        print(f"    - Removing {len(cols_to_remove)} unwanted columns")
        df = df.drop(columns=cols_to_remove)
    
    # 8. Map ratings to stars
    print(f"    - Mapping ratings to stars")
    df = apply_rating_mappings(df)
    
    # 9. Standardize text values
    print(f"    - Standardizing text values")
    df = apply_text_standardization(df)
    
    return df


def apply_rating_mappings(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply rating mappings to convert text ratings to star ratings.
    
    Maps:
    - Trè byen → 5 Etwal
    - Byen → 4 Etwal
    - Pasab → 3 Etwal
    - Pa bon → 2 Etwal
    - Pat bon ditou → 1 Etwal
    
    Also maps duration and frequency ratings.
    
    Args:
        df: DataFrame to transform
    
    Returns:
        pd.DataFrame: DataFrame with mapped ratings
    """
    # Apply mappings to all string columns
    for col in df.columns:
        if df[col].dtype == 'object':  # String columns
            # Apply each mapping
            for original, replacement in RATING_MAPPINGS.items():
                df[col] = df[col].astype(str).str.replace(
                    original, replacement, regex=False
                )
    
    return df


def apply_text_standardization(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize text values (e.g., Enfimyè, Miss, etc.).
    
    Args:
        df: DataFrame to transform
    
    Returns:
        pd.DataFrame: DataFrame with standardized text
    """
    # Apply standardization to all string columns
    for col in df.columns:
        if df[col].dtype == 'object':  # String columns
            for original, replacement in TEXT_STANDARDIZATION.items():
                df[col] = df[col].astype(str).str.replace(
                    original, replacement, regex=False
                )
    
    return df


def get_access_columns(df: pd.DataFrame) -> List[str]:
    """
    Get the list of columns to be written to Access database.
    
    This filters out temporary/intermediate columns and keeps only
    the final columns needed for Power BI.
    
    Args:
        df: Transformed DataFrame
    
    Returns:
        List[str]: List of column names to write to Access
    """
    # Define columns to exclude (intermediate/temporary columns)
    exclude_patterns = ['_temp', '_intermediate']
    
    # Get all columns except those matching exclude patterns
    access_cols = [
        col for col in df.columns
        if not any(pattern in col.lower() for pattern in exclude_patterns)
    ]
    
    return access_cols


if __name__ == "__main__":
    # Test the transformer
    from etl.extractor import load_csv
    
    df = load_csv()
    df_transformed = transform(df)
    
    print(f"\n📊 Transformation Summary:")
    print(f"  - Input shape: {df.shape}")
    print(f"  - Output shape: {df_transformed.shape}")
    print(f"  - Columns removed: {len(df.columns) - len(df_transformed.columns)}")
    print(f"\n📋 Final columns:")
    for i, col in enumerate(df_transformed.columns, 1):
        print(f"  {i}. {col}")
