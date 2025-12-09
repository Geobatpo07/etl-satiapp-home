"""
Uploader module for ETL pipeline.

This module handles uploading the Excel file to SharePoint using the Microsoft Graph API.
"""

import requests
from pathlib import Path
from config.settings import (
    EXCEL_OUTPUT,
    ACCESS_OUTPUT,  # Legacy support
    SHAREPOINT_SITE,
    SITE_NAME,
    DOCUMENT_LIBRARY,
    ACCESS_TOKEN
)


def upload_to_sharepoint(
    file_path: Path = EXCEL_OUTPUT,
    overwrite: bool = True
) -> dict:
    """
    Upload Excel file to SharePoint using Microsoft Graph API.
    
    This function:
    1. Reads the Excel file
    2. Uploads it to SharePoint using the Graph API
    3. Supports overwriting existing files
    
    Args:
        file_path: Path to the Excel file (defaults to EXCEL_OUTPUT)
        overwrite: Whether to overwrite existing file (default: True)
    
    Returns:
        dict: Response from SharePoint API
    
    Raises:
        FileNotFoundError: If the Excel file doesn't exist
        requests.HTTPError: If the upload fails
    
    Example:
        >>> upload_to_sharepoint()
        >>> # File uploaded to SharePoint!
    """
    print(f"\nUploading to SharePoint...")
    
    # Validate file exists
    if not file_path.exists():
        raise FileNotFoundError(f"Excel file not found: {file_path}")
    
    # Get file info
    file_name = file_path.name
    file_size = file_path.stat().st_size
    file_size_mb = file_size / (1024 * 1024)
    
    print(f"  File: {file_name}")
    print(f"  Size: {file_size_mb:.2f} MB")
    
    # Read file content
    with open(file_path, 'rb') as f:
        file_content = f.read()
    
    # Upload to SharePoint
    try:
        # For small files (< 4MB), use simple upload
        if file_size < 4 * 1024 * 1024:
            print(f"  Using simple upload (file < 4MB)...")
            response = simple_upload(file_name, file_content, overwrite)
        else:
            # For larger files, use chunked upload
            print(f" Using chunked upload (file >= 4MB)...")
            response = chunked_upload(file_name, file_content, overwrite)
        
        print(f"File successfully uploaded to SharePoint!")
        print(f"  Location: {SHAREPOINT_SITE}/{DOCUMENT_LIBRARY}/{file_name}")
        
        return response
        
    except requests.HTTPError as e:
        print(f"Upload failed: {e}")
        print(f"  Response: {e.response.text if hasattr(e, 'response') else 'No response'}")
        raise


def simple_upload(file_name: str, file_content: bytes, overwrite: bool) -> dict:
    """
    Upload file using simple upload (for files < 4MB).
    
    Uses the SharePoint REST API endpoint:
    PUT /sites/{site}/_api/web/GetFolderByServerRelativeUrl('{library}')/Files/Add
    
    Args:
        file_name: Name of the file
        file_content: File content as bytes
        overwrite: Whether to overwrite existing file
    
    Returns:
        dict: API response
    """
    # Build the upload URL
    # Using Graph API for modern authentication
    upload_url = (
        f"{SHAREPOINT_SITE}/_api/web/"
        f"GetFolderByServerRelativeUrl('/sites/{SITE_NAME}/{DOCUMENT_LIBRARY}')/"
        f"Files/Add(url='{file_name}', overwrite={str(overwrite).lower()})"
    )
    
    # Set headers
    headers = {
        'Authorization': f'Bearer {ACCESS_TOKEN}',
        'Accept': 'application/json;odata=verbose',
        'Content-Type': 'application/octet-stream',
    }
    
    # Upload file
    print(f"  Uploading to: {upload_url}")
    response = requests.post(upload_url, headers=headers, data=file_content)
    response.raise_for_status()
    
    return response.json()


def chunked_upload(file_name: str, file_content: bytes, overwrite: bool) -> dict:
    """
    Upload file using chunked upload (for files >= 4MB).
    
    Uses Microsoft Graph API large file upload:
    1. Create upload session
    2. Upload chunks
    3. Complete upload
    
    Args:
        file_name: Name of the file
        file_content: File content as bytes
        overwrite: Whether to overwrite existing file
    
    Returns:
        dict: API response
    """
    # Graph API endpoint for creating upload session
    # First, get the site ID
    site_id = get_site_id()
    
    # Create upload session
    session_url = (
        f"https://graph.microsoft.com/v1.0/sites/{site_id}/"
        f"drive/root:/{DOCUMENT_LIBRARY}/{file_name}:/createUploadSession"
    )
    
    headers = {
        'Authorization': f'Bearer {ACCESS_TOKEN}',
        'Content-Type': 'application/json',
    }
    
    session_data = {
        "item": {
            "@microsoft.graph.conflictBehavior": "replace" if overwrite else "fail"
        }
    }
    
    print(f"   Creating upload session...")
    response = requests.post(session_url, headers=headers, json=session_data)
    response.raise_for_status()
    upload_url = response.json()['uploadUrl']
    
    # Upload in chunks (recommended chunk size: 5-10 MB)
    chunk_size = 5 * 1024 * 1024  # 5 MB
    file_size = len(file_content)
    chunks_total = (file_size + chunk_size - 1) // chunk_size
    
    print(f"   Uploading {chunks_total} chunks...")
    
    for i in range(0, file_size, chunk_size):
        chunk = file_content[i:i + chunk_size]
        chunk_start = i
        chunk_end = min(i + chunk_size, file_size) - 1
        
        chunk_headers = {
            'Content-Length': str(len(chunk)),
            'Content-Range': f'bytes {chunk_start}-{chunk_end}/{file_size}',
        }
        
        chunk_response = requests.put(upload_url, headers=chunk_headers, data=chunk)
        chunk_response.raise_for_status()
        
        chunk_num = (i // chunk_size) + 1
        print(f"      Chunk {chunk_num}/{chunks_total} uploaded")
    
    return chunk_response.json()


def get_site_id() -> str:
    """
    Get SharePoint site ID using Graph API.
    
    Returns:
        str: Site ID
    """
    # Extract site name from URL
    # SHAREPOINT_SITE format: https://company.sharepoint.com/sites/site-name
    
    headers = {
        'Authorization': f'Bearer {ACCESS_TOKEN}',
        'Accept': 'application/json',
    }
    
    # Get site by URL
    site_url = f"https://graph.microsoft.com/v1.0/sites/{SHAREPOINT_SITE.split('//')[1]}"
    response = requests.get(site_url, headers=headers)
    response.raise_for_status()
    
    return response.json()['id']


def verify_sharepoint_upload(file_name: str = None) -> bool:
    """
    Verify that the file was successfully uploaded to SharePoint.
    
    Args:
        file_name: Name of the file to verify (defaults to Excel file name)
    
    Returns:
        bool: True if file exists on SharePoint, False otherwise
    """
    if file_name is None:
        file_name = EXCEL_OUTPUT.name
    
    print(f"\nVerifying SharePoint upload...")
    
    try:
        site_id = get_site_id()
        
        # Check if file exists
        file_url = (
            f"https://graph.microsoft.com/v1.0/sites/{site_id}/"
            f"drive/root:/{DOCUMENT_LIBRARY}/{file_name}"
        )
        
        headers = {
            'Authorization': f'Bearer {ACCESS_TOKEN}',
            'Accept': 'application/json',
        }
        
        response = requests.get(file_url, headers=headers)
        
        if response.status_code == 200:
            file_info = response.json()
            print(f"  File found on SharePoint!")
            print(f"    - Name: {file_info.get('name')}")
            print(f"    - Size: {file_info.get('size', 0) / (1024*1024):.2f} MB")
            print(f"    - Modified: {file_info.get('lastModifiedDateTime')}")
            return True
        else:
            print(f"  File not found on SharePoint")
            return False
            
    except Exception as e:
        print(f"  Verification failed: {e}")
        return False


if __name__ == "__main__":
    # Test the uploader
    upload_to_sharepoint()
    verify_sharepoint_upload()
