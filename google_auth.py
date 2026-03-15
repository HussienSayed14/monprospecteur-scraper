"""
google_auth.py
──────────────
Single shared authentication module for both Google Drive and Sheets.
Both drive_uploader.py and sheets_uploader.py import from here.

This ensures:
- Both scopes are always requested together
- Token is refreshed once with all scopes intact
- No scope dropping on refresh
"""

import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# ── Both scopes together — never split them ───────────────────────────────────
SCOPES = [
    "https://www.googleapis.com/auth/drive.file",
    "https://www.googleapis.com/auth/spreadsheets",
]

CREDENTIALS_FILE = os.getenv("GOOGLE_CREDENTIALS_FILE", "credentials.json")
TOKEN_FILE       = os.getenv("GOOGLE_TOKEN_FILE",        "token.json")


class GoogleAuthError(Exception):
    """Raised when Google auth fails and cannot be recovered automatically."""
    pass


def get_credentials():
    """
    Returns valid Google credentials with both Drive and Sheets scopes.

    Flow:
    1. token.json exists + valid + has all scopes → reuse silently
    2. token.json exists + expired → auto-refresh silently (keeps all scopes)
    3. token.json missing, corrupt, or missing scopes:
       - Local machine → opens browser for re-auth
       - Docker container → raises GoogleAuthError with fix instructions
    """
    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow

    creds = None

    if Path(TOKEN_FILE).exists():
        try:
            creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
        except Exception:
            print("  ⚠️  token.json is corrupt — will re-authenticate")
            Path(TOKEN_FILE).unlink(missing_ok=True)
            creds = None

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
                Path(TOKEN_FILE).write_text(creds.to_json())
                print("  🔄 Google token refreshed automatically")
            except Exception as e:
                # Refresh failed — need full re-auth
                print(f"  ⚠️  Token refresh failed: {e} — will re-authenticate")
                Path(TOKEN_FILE).unlink(missing_ok=True)
                creds = None

    if not creds or not creds.valid:
        # Need full browser re-auth
        if not Path(CREDENTIALS_FILE).exists():
            raise GoogleAuthError(
                f"credentials.json not found at: {CREDENTIALS_FILE}\n"
                f"Download it from Google Cloud Console → APIs & Services → Credentials"
            )

        in_docker = os.path.exists("/.dockerenv") or os.getenv("PLAYWRIGHT_HEADLESS", "").lower() == "true"
        if in_docker:
            raise GoogleAuthError(
                f"Running in Docker — cannot open browser for re-auth.\n"
                f"Fix: On your LOCAL machine run: python sheets_uploader.py\n"
                f"Then copy the generated token.json to the server."
            )

        print("  🔐 Opening browser for Google authentication...")
        flow  = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
        creds = flow.run_local_server(port=0)
        Path(TOKEN_FILE).write_text(creds.to_json())
        print(f"  ✅ Authenticated — token saved to {TOKEN_FILE}")
        print(f"  ✅ Scopes: {creds.scopes}")

    return creds


def get_drive_service():
    """Return an authenticated Google Drive API service."""
    from googleapiclient.discovery import build
    return build("drive", "v3", credentials=get_credentials())


def get_sheets_service():
    """Return an authenticated Google Sheets API service."""
    from googleapiclient.discovery import build
    return build("sheets", "v4", credentials=get_credentials())