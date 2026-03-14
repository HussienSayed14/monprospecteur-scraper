"""
drive_uploader.py
─────────────────
Uploads PDFs to Google Drive with this folder structure:

    {Root Folder}/
        {Year}/                          e.g. 2026/
            {Lead Type}/                 e.g. Succession / 60Daysnotice / Vpti
                {Street Number + Street Name}/   e.g. 10672 Avenue De Lorimier/
                    original-act.pdf
                    print.pdf

Setup (one-time):
    1. Go to https://console.cloud.google.com
    2. Create a project → enable Google Drive API
    3. Create OAuth2 credentials (Desktop app) → download as credentials.json
    4. Place credentials.json in this directory
    5. First run opens browser for auth → saves token.json for future runs
    6. Set DRIVE_ROOT_FOLDER_ID in .env to the ID of your root Drive folder
       (get it from the URL when you open the folder:
        https://drive.google.com/drive/folders/THIS_IS_THE_ID)

    pip install google-auth google-auth-oauthlib google-api-python-client

Usage:
    from drive_uploader import upload_lead_files

    drive_url = upload_lead_files(
        doc_id     = "5eae5b61209a761738f4c776",
        lead_type  = "Succession",          # from Lead Source field
        address    = "10672 Avenue De Lorimier, Montreal",
        street_num = "10672",
        street     = "Avenue De Lorimier",
        act_pdf    = "output/pdfs/5eae5b61209a761738f4c776.pdf",
        print_pdf  = "output/prints/5eae5b61209a761738f4c776_print.pdf",
    )
    # drive_url is the folder URL — put this in the Google Drive column
"""

import os
from dotenv import load_dotenv

load_dotenv()
import re
from pathlib import Path
from datetime import datetime

try:
    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaFileUpload
except ImportError:
    raise ImportError("Run: pip install google-auth google-auth-oauthlib google-api-python-client")

SCOPES           = ["https://www.googleapis.com/auth/drive.file"]
CREDENTIALS_FILE = os.getenv("GOOGLE_CREDENTIALS_FILE", "credentials.json")
TOKEN_FILE       = os.getenv("GOOGLE_TOKEN_FILE", "token.json")
ROOT_FOLDER_ID   = os.getenv("DRIVE_ROOT_FOLDER_ID", "")

# Lead type slug → folder name mapping (matches Lead Source values)
LEAD_TYPE_FOLDER = {
    "Succession":    "Succession",
    "60Daysnotice":  "60Daysnotice",
    "Vpti":          "Vpti",
}


# ── Auth ──────────────────────────────────────────────────────────────────────

class GoogleAuthError(Exception):
    """Raised when Google auth fails and cannot be recovered automatically."""
    pass


def _get_service():
    creds = None

    if Path(TOKEN_FILE).exists():
        try:
            creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
        except Exception as e:
            raise GoogleAuthError(
                f"token.json is corrupt or missing required scopes: {e}\n"
                f"Fix: delete token.json and run: python sheets_uploader.py"
            )

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
                Path(TOKEN_FILE).write_text(creds.to_json())
                print("  🔄 Google token refreshed automatically")
            except Exception as e:
                raise GoogleAuthError(
                    f"Token refresh failed: {e}\n"
                    f"Fix: delete token.json and run: python sheets_uploader.py"
                )
        else:
            raise GoogleAuthError(
                f"No valid token.json found.\n"
                f"Fix: delete token.json and run: python sheets_uploader.py"
            )

    return build("drive", "v3", credentials=creds)


# ── Folder helpers ────────────────────────────────────────────────────────────

def _escape_drive_query(value: str) -> str:
    """
    Escape a string for use in a Drive API query.
    Single quotes must be escaped as \' in Drive query strings.
    """
    return value.replace("\\", "\\\\").replace("'", "\\'")


def _get_or_create_folder(service, name: str, parent_id: str) -> str:
    """Return folder ID for `name` inside `parent_id`, creating it if needed."""
    escaped_name = _escape_drive_query(name)
    q = (
        f"name='{escaped_name}' "
        f"and mimeType='application/vnd.google-apps.folder' "
        f"and '{parent_id}' in parents "
        f"and trashed=false"
    )
    res = service.files().list(q=q, fields="files(id)").execute()
    files = res.get("files", [])
    if files:
        return files[0]["id"]

    meta = {
        "name":     name,  # actual name, not escaped
        "mimeType": "application/vnd.google-apps.folder",
        "parents":  [parent_id],
    }
    folder = service.files().create(body=meta, fields="id").execute()
    return folder["id"]


def _get_folder_url(folder_id: str) -> str:
    return f"https://drive.google.com/drive/folders/{folder_id}"


def _sanitize_folder_name(name: str) -> str:
    """Remove characters not allowed in Drive folder names."""
    return re.sub(r'[\\/:*?"<>|]', "-", name).strip()


# ── File upload ───────────────────────────────────────────────────────────────

def _upload_file(service, file_path: str, folder_id: str, file_name: str) -> str:
    """Upload a PDF file. Returns its web view URL."""
    media    = MediaFileUpload(file_path, mimetype="application/pdf", resumable=True)
    metadata = {"name": file_name, "parents": [folder_id]}
    uploaded = service.files().create(
        body=metadata, media_body=media, fields="id,webViewLink"
    ).execute()

    # Make viewable by anyone with the link
    service.permissions().create(
        fileId=uploaded["id"],
        body={"type": "anyone", "role": "reader"},
    ).execute()

    return uploaded.get("webViewLink", "")


# ── Main entry point ──────────────────────────────────────────────────────────

def upload_lead_files(
    doc_id:     str,
    lead_type:  str,    # "Succession" | "60Daysnotice" | "Vpti"
    street_num: str,    # "10672"
    street:     str,    # "Avenue De Lorimier"
    act_pdf:    str,    # local path to original act PDF
    print_pdf:  str,    # local path to print PDF
    year:       str = None, # type: ignore
    root_folder_id: str = None, # type: ignore
) -> str:
    """
    Upload act PDF and print PDF for one lead.

    Folder structure created automatically:
        Root/
            {year}/
                {lead_type}/
                    {street_num} {street}/
                        original-act.pdf
                        print.pdf

    Returns:
        URL of the property folder (for the Google Drive column in Excel)
    """
    root_folder_id = root_folder_id or ROOT_FOLDER_ID
    if not root_folder_id:
        raise ValueError("Set DRIVE_ROOT_FOLDER_ID in .env or pass root_folder_id")

    year = year or str(datetime.utcnow().year)
    type_folder_name = LEAD_TYPE_FOLDER.get(lead_type, lead_type)
    property_folder_name = _sanitize_folder_name(f"{street_num} {street}")

    service = _get_service()

    # Build folder path: Root → Year → Type → Property
    year_folder_id     = _get_or_create_folder(service, year,                root_folder_id)
    type_folder_id     = _get_or_create_folder(service, type_folder_name,    year_folder_id)
    property_folder_id = _get_or_create_folder(service, property_folder_name, type_folder_id)

    folder_url = _get_folder_url(property_folder_id)
    print(f"  📁 Drive folder: Root/{year}/{type_folder_name}/{property_folder_name}")

    # Upload act PDF
    if act_pdf and Path(act_pdf).exists():
        print(f"  → Uploading original-act.pdf ...")
        _upload_file(service, act_pdf, property_folder_id, "original-act.pdf")
    else:
        print(f"  ⚠️  Act PDF not found: {act_pdf}")

    # Upload print PDF
    if print_pdf and Path(print_pdf).exists():
        print(f"  → Uploading print.pdf ...")
        _upload_file(service, print_pdf, property_folder_id, "print.pdf")
    else:
        print(f"  ⚠️  Print PDF not found: {print_pdf}")

    print(f"  ✅ Drive folder URL: {folder_url}")
    return folder_url


def upload_excel(excel_path: str, root_folder_id: str = None) -> str: # type: ignore
    """Upload the leads Excel file to the root Drive folder. Returns URL."""
    root_folder_id = root_folder_id or ROOT_FOLDER_ID
    service  = _get_service()
    path     = Path(excel_path)
    media    = MediaFileUpload(str(path), mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", resumable=True)
    metadata = {"name": path.name, "parents": [root_folder_id]}
    uploaded = service.files().create(body=metadata, media_body=media, fields="id,webViewLink").execute()
    service.permissions().create(
        fileId=uploaded["id"],
        body={"type": "anyone", "role": "reader"},
    ).execute()
    url = uploaded.get("webViewLink", "")
    print(f"  ✅ Excel uploaded → {url}")
    return url


# ── Auth test ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("Testing Google Drive connection...")
    service = _get_service()
    res = service.files().list(pageSize=5, fields="files(id,name,mimeType)").execute()
    print("✅ Connected. Recent files:")
    for f in res.get("files", []):
        print(f"  [{f['mimeType'].split('.')[-1]}] {f['name']}  ({f['id']})")
    print(f"\nRoot folder ID from .env: '{ROOT_FOLDER_ID}'")
    if ROOT_FOLDER_ID:
        print("✅ Root folder ID is set")
    else:
        print("⚠️  DRIVE_ROOT_FOLDER_ID is not set in .env")