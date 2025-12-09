"""
ETL SATIAP Home - Main Entry Point

This script orchestrates the complete ETL pipeline:
1. Extract: Load CSV data
2. Transform: Apply VBA and Power Query transformations
3. Load: Write to Access database
4. Upload: Upload to SharePoint

Usage:
    uv run etl.py
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from etl.extractor import load_csv
from etl.transformer import transform
from etl.loader import write_to_excel, verify_excel_data
from etl.uploader import upload_to_sharepoint, verify_sharepoint_upload
from etl.validator import save_to_staging, validate_columns, reorder_columns
from config.settings import validate_settings, STAGING_DB_PATH, EXCEL_OUTPUT


def run_etl():
    """
    Execute the complete ETL pipeline.
    
    Steps:
    1. Validate configuration
    2. Extract data from CSV
    3. Transform data (VBA + Power Query)
    4. Load data to Access database
    5. Upload Access database to SharePoint
    
    Returns:
        bool: True if pipeline completed successfully, False otherwise
    """
    print("=" * 80)
    print("🚀 ETL SATIAP HOME - Pipeline Starting")
    print("=" * 80)
    
    try:
        # Step 0: Validate configuration
        print("\n📋 Step 0: Validating configuration...")
        try:
            validate_settings()
            print("  ✅ Configuration validated")
        except ValueError as e:
            print(f"  ⚠️  Configuration warnings: {e}")
            print("  ℹ️  Continuing anyway (some features may not work)")
        
        # Step 1: Extract
        print("\n📥 Step 1: Extracting data from CSV...")
        df = load_csv()
        print(f"  ✅ Extracted {len(df)} rows, {len(df.columns)} columns")
        
        # Step 2: Transform
        print("\n🔄 Step 2: Transforming data...")
        df_transformed = transform(df)
        print(f"  ✅ Transformed to {len(df_transformed)} rows, {len(df_transformed.columns)} columns")
        
        # Step 3: Staging and Validation
        print("\n🛡️ Step 3: Staging and Validation...")
        
        # Save to SQLite
        save_to_staging(df_transformed)
        
        # Validate Columns
        is_valid, errors = validate_columns(df_transformed)
        
        if not is_valid:
            print("  ❌ Validation FAILED!")
            for err in errors:
                print(f"    - {err}")
            print("\n⛔ Pipeline stopped due to validation errors.")
            return False
            
        print("  ✅ Column schema validation passed")

        # Reorder columns to match expected schema (keep any extras at the end)
        df_transformed = reorder_columns(df_transformed)
        print("  🔀 Columns reordered to expected schema")

        # Step 4: Load to Excel
        print("\n💾 Step 4: Loading data to Excel file...")
        write_to_excel(df_transformed)
        print(f"  ✅ Data loaded to Excel")
        
        # Verify Excel data
        verify_excel_data()
        
        # Step 5: Upload to SharePoint
        print("\n☁️  Step 5: Uploading to SharePoint...")
        try:
            upload_to_sharepoint()
            print(f"  ✅ Uploaded to SharePoint")
            
            # Verify upload
            verify_sharepoint_upload()
        except Exception as e:
            print(f"  ⚠️  SharePoint upload failed: {e}")
            print(f"  ℹ️  Excel file was created successfully at: {Path('data/output/output.xlsx').absolute()}")
            print(f"  ℹ️  You can manually upload it to SharePoint")
        
        # Success!
        print("\n" + "=" * 80)
        print("✅ ETL PIPELINE COMPLETED SUCCESSFULLY!")
        print("=" * 80)
        print(f"\n📊 Summary:")
        print(f"  - Rows processed: {len(df_transformed)}")
        print(f"  - Staging DB: {STAGING_DB_PATH}")
        print(f"  - Excel file: {EXCEL_OUTPUT}")
        print(f"  - SharePoint: Uploaded (if configured)")
        print("\n")
        
        return True
        
    except FileNotFoundError as e:
        print(f"\n❌ ERROR: File not found - {e}")
        print(f"  ℹ️  Make sure 'data/datafeadback.csv' exists")
        return False
        
    except Exception as e:
        print(f"\n❌ ERROR: Pipeline failed - {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """
    Main entry point for the ETL pipeline.
    
    This function is called when running: uv run etl.py
    """
    success = run_etl()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
