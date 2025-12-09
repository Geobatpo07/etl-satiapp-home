"""
Configuration settings for ETL SATIAP Home pipeline.

This module contains all configuration parameters for the ETL process including:
- File paths for CSV input and Access output
- SharePoint connection settings
- Authentication tokens

Before running the pipeline, ensure all settings are properly configured.
"""

import os
from pathlib import Path

# Base directory of the project
BASE_DIR = Path(__file__).resolve().parent.parent

# ============================================================================
# FILE PATHS
# ============================================================================

# Path to the input CSV file containing feedback data
CSV_PATH = BASE_DIR / "data" / "datafeadback.csv"

# Path to the output Excel file
EXCEL_OUTPUT = BASE_DIR / "data" / "output" / "output.xlsx"

# Legacy: Path to the output Access database (if needed)
ACCESS_OUTPUT = BASE_DIR / "data" / "output.accdb"

# ============================================================================
# SHAREPOINT CONFIGURATION
# ============================================================================

# SharePoint site URL (example: https://yourcompany.sharepoint.com/sites/your-site)
SHAREPOINT_SITE = "https://votreentreprise.sharepoint.com/sites/votre-site"

# SharePoint site name (the part after /sites/)
SITE_NAME = "votre-site"

# Document library where the Access file will be uploaded
DOCUMENT_LIBRARY = "Documents partages"

# ============================================================================
# AUTHENTICATION
# ============================================================================

# Microsoft Graph API access token
# To obtain a token:
# 1. Register an app in Azure AD
# 2. Grant Sites.ReadWrite.All permissions
# 3. Generate an access token
# 4. Paste the token here
ACCESS_TOKEN = os.getenv("SHAREPOINT_ACCESS_TOKEN", "votre-token-graph-api")

# ============================================================================
# EXCEL CONFIGURATION
# ============================================================================

# Name of the sheet in Excel workbook
EXCEL_SHEET_NAME = "Source_Raw"

# ============================================================================
# ACCESS DATABASE CONFIGURATION (Legacy)
# ============================================================================

# Name of the table in Access database
ACCESS_TABLE_NAME = "Source_Raw"

# Access ODBC driver (usually pre-installed on Windows)
ACCESS_DRIVER = "Microsoft Access Driver (*.mdb, *.accdb)"

# ============================================================================
# VALIDATION
# ============================================================================

def validate_settings():
    """
    Validate that all required settings are properly configured.
    
    Raises:
        ValueError: If any required setting is missing or invalid
    """
    errors = []
    
    # Check CSV path
    if not CSV_PATH.exists():
        errors.append(f"CSV file not found: {CSV_PATH}")
    
    # Check data directory exists
    data_dir = BASE_DIR / "data"
    if not data_dir.exists():
        errors.append(f"Data directory not found: {data_dir}")
    
    # Check that output directory is writable
    if not EXCEL_OUTPUT.parent.exists():
        errors.append(f"Output directory not found: {EXCEL_OUTPUT.parent}")
    
    # Check SharePoint settings
    if SHAREPOINT_SITE == "https://votreentreprise.sharepoint.com/sites/votre-site":
        errors.append("SHAREPOINT_SITE must be configured with your actual SharePoint URL")
    
    if ACCESS_TOKEN == "votre-token-graph-api":
        errors.append("ACCESS_TOKEN must be configured with a valid Graph API token")
    
    if errors:
        raise ValueError(
            "Configuration errors found:\n" + "\n".join(f"  - {error}" for error in errors)
        )

# ============================================================================
# COLUMN MAPPINGS (for Power Query transformations)
# ============================================================================

# Rating mappings for satisfaction levels
RATING_MAPPINGS = {
    # 5-star ratings
    "Trè byen": "5 Etwal",
    "Byen": "4 Etwal",
    "Pasab": "3 Etwal",
    "Pa bon": "2 Etwal",
    "Pat bon ditou": "1 Etwal",
    
    # Duration ratings
    "Trè long": "Trè long",
    "Long": "Long",
    "Kout": "Kout",
    "Trè kout": "Trè kout",
    
    # Frequency ratings
    "Pa ditou": "Pa ditou",
    "Trè raman": "Trè raman",
    "raman": "raman",
    "Pa souvan": "Pa souvan",
    "Souvan": "Souvan",
}

# Text standardization mappings
TEXT_STANDARDIZATION = {
    "Enfimyè": "Enfimyè",
    "Miss": "Miss",
    # Add more standardization rules as needed
}

# Columns to remove (Power Query step)
COLUMNS_TO_REMOVE = [
    "Email Address",
    "First Name",
    "Last Name",
    "Custom Data 1",
    "language",
]

# Encoding corrections mapping
ENCODING_FIXES = {
    "Ã´": "ô",
    "Ã©": "é",
    "Ã¨": "è",
    "Ã ": "à",
    "Ã®": "î",
    "Ã§": "ç",
    "Ã»": "û",
    "Ã¢": "â",
    "Ã«": "ë",
    "Ã¯": "ï",
    "Ã¼": "ü",
    "Ã¶": "ö",
}

# ============================================================================
# STAGING AND VALIDATION
# ============================================================================

# Path to SQLite staging database
STAGING_DB_PATH = BASE_DIR / "data" / "staging.db"

# Expected Columns for Validation
EXPECTED_COLUMNS = [
    "Respondent ID", "Collector ID", "Start Date", "End Date", "IP Address",
    "Nan ki depatman lopital / sant sante a ye ?",
    "Nan ki lopital oubyen sant sante ou konn ale nan depatmant atibonit ?",
    "Nan ki lopital oubyen sant sante ou konn ale nan depatman sant?",
    "Nan ki lopital oubyen sant sante ou konn ale nan depatman grandans?",
    "Nan ki lopital oubyen sant sante ou konn ale nan depatman nip?",
    "Nan ki lopital oubyen sant sante ou konn ale nan depatman nò?",
    "Nan ki lopital oubyen sant sante ou konn ale nan depatman nòdès?",
    "Nan ki lopital oubyen sant sante ou konn ale nan depatman nòdwès?",
    "Nan ki lopital oubyen sant sante ou konn ale nan depatman wès?",
    "Nan ki lopital oubyen sant sante ou konn ale nan depatman sid?",
    "Nan ki lopital oubyen sant sante ou konn ale nan depatman sidès?",
    "Antre kòd ST?",
    "Dat dènye vizit ou nan sant sante a oubyen lopital la ?",
    "Kòman ou tap note sèvis ou resevwa nan sant sante a oubyen lopital la jeneralman ?",
    "De kisa ou satisfè nan sant sante a oubyen lopital la?",
    "Unnamed: 32", "Unnamed: 33", "Unnamed: 34", "Unnamed: 35",
    "Kòmantè ou .",
    "Kòman ou tap note pwòprete nan sant sante a oubyen lopital la?",
    "Kòman ou tap note akèy nan sant sante a oubyen lopital la?",
    "Kòman ou tap note sèvis pèsonèl yo bay nan sant sante a oubyen lopital la jeneralman ?",
    "Kòman ou tap note tan ou fè ap tann nan sant sante a oubyen lopital la jeneralman ?",
    "Eske moun yo mal gade ou nan sant sante a oubyen lopital la jeneralman ?",
    "Ki moun ki mal gade w nan sant sante oubyen lopital la ?",
    "Kòmantè ou sou sant sante a oubyen lopital la.",
    "Ki pèsonèl ou pa satisfè de sèvis li ?",
    "Unnamed: 53", "Unnamed: 54", "Unnamed: 55", "Unnamed: 56",
    "Kòmantè e sigjesyon ou sou sèvis ou jwenn?",
    "Kòman ou tap note sèvis Doktè yo bay nan sant sante a oubyen lopital la jeneralman ?",
    "Kòman ou tap note sèvis Enfimyè yo bay nan sant sante a oubyen lopital la jeneralman ?",
    "Kòman ou tap note sèvis Famasyen yo bay nan sant sante a oubyen lopital la jeneralman ?",
    "Kòman ou tap note sèvis Sikològ / travayè sosyalyo bay nan sant sante a oubyen lopital la jeneralman ?",
    "Kòman ou tap note sèvis Laboratwa yo bay nan sant sante a oubyen lopital la jeneralman ?",
    "Pou ki sèvis ou bay enfòmasyon sa yo ",
    "Unnamed: 22", "Unnamed: 23", "Unnamed: 24", "Unnamed: 25", "Unnamed: 26",
    "Ki moun ki mal gade w nan sant sante oubyen lopital la ?",
    "Unnamed: 43", "Unnamed: 44", "Unnamed: 45", "Unnamed: 46", "Unnamed: 47", "Unnamed: 48", "Unnamed: 49", "Unnamed: 50",
    "Hospital_Combined",
    "Satisfaction_Combined",
    "Dissatisfaction_Combined",
    "Mistreatment_Combined"
]
