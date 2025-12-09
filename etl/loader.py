"""
Loader module for ETL pipeline - Excel version.

This module handles writing the transformed DataFrame to an Excel file.
"""

import pandas as pd
from pathlib import Path
from config.settings import EXCEL_OUTPUT, EXCEL_SHEET_NAME


def write_to_excel(df: pd.DataFrame, excel_path: Path = EXCEL_OUTPUT) -> None:
    """
    Write DataFrame to Excel file.
    
    This function:
    1. Creates the Excel file
    2. Writes the DataFrame to a sheet named 'Source_Raw'
    3. Applies basic formatting for readability
    
    Args:
        df: DataFrame to write to Excel
        excel_path: Path to Excel file
    
    Raises:
        Exception: If there's an error writing to Excel
    
    Example:
        >>> df_transformed = transform(df)
        >>> write_to_excel(df_transformed)
    """
    print(f"\n💾 Writing data to Excel file: {excel_path}")
    
    # Ensure the data directory exists
    excel_path.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        # Write to Excel with formatting
        print(f"  📊 Creating Excel file with {len(df)} rows and {len(df.columns)} columns...")
        
        with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
            # Write the main data sheet
            df.to_excel(
                writer,
                sheet_name=EXCEL_SHEET_NAME,
                index=False,
                freeze_panes=(1, 0)  # Freeze the header row
            )
            
            # Get the worksheet to apply formatting
            worksheet = writer.sheets[EXCEL_SHEET_NAME]
            
            # Auto-adjust column widths
            print(f"  🎨 Applying formatting...")
            for column in worksheet.columns:
                max_length = 0
                column_letter = column[0].column_letter
                
                for cell in column:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass
                
                # Set column width (with a maximum of 50)
                adjusted_width = min(max_length + 2, 50)
                worksheet.column_dimensions[column_letter].width = adjusted_width
            
            # Make header row bold
            from openpyxl.styles import Font, PatternFill
            
            header_font = Font(bold=True, color="FFFFFF")
            header_fill = PatternFill(start_color="366092", end_color="366092", fill_type="solid")
            
            for cell in worksheet[1]:
                cell.font = header_font
                cell.fill = header_fill
        
        print(f"✅ Data successfully written to Excel file!")
        print(f"  📊 Sheet: {EXCEL_SHEET_NAME}")
        print(f"  📈 Rows: {len(df)}")
        print(f"  📋 Columns: {len(df.columns)}")
        print(f"  📁 File size: {excel_path.stat().st_size / 1024:.2f} KB")
        
    except Exception as e:
        print(f"❌ Error writing to Excel file: {e}")
        raise


def verify_excel_data(excel_path: Path = EXCEL_OUTPUT, sheet_name: str = EXCEL_SHEET_NAME) -> None:
    """
    Verify data was written correctly to Excel file.
    
    Args:
        excel_path: Path to Excel file
        sheet_name: Name of the sheet to verify
    """
    print(f"\n🔍 Verifying Excel file...")
    
    try:
        # Read the Excel file
        df = pd.read_excel(excel_path, sheet_name=sheet_name)
        
        print(f"  ✅ Verification successful!")
        print(f"    - Rows: {len(df)}")
        print(f"    - Columns: {len(df.columns)}")
        print(f"    - File exists: {excel_path.exists()}")
        
        # Show first few rows
        print(f"\n  📋 Sample data (first 3 rows):")
        print(df.head(3).to_string(index=False, max_cols=5))
        
    except Exception as e:
        print(f"  ❌ Verification failed: {e}")


# Backward compatibility: keep the Access function but make it call Excel
def write_to_access(df: pd.DataFrame, db_path: Path = None) -> None:
    """
    Legacy function for backward compatibility.
    Now writes to Excel instead of Access.
    
    Args:
        df: DataFrame to write
        db_path: Ignored (kept for compatibility)
    """
    print("  ℹ️  Note: write_to_access() now writes to Excel format")
    write_to_excel(df)


if __name__ == "__main__":
    # Test the loader
    from etl.extractor import load_csv
    from etl.transformer import transform
    
    df = load_csv()
    df_transformed = transform(df)
    write_to_excel(df_transformed)
    verify_excel_data()
