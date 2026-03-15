"""
sheets_uploader.py
──────────────────
Appends lead rows directly to your Google Sheet.

Setup (one-time):
    1. Same credentials.json from Google Cloud Console (Desktop OAuth app)
    2. Enable BOTH APIs in Google Cloud:
         - Google Drive API  (already done for drive_uploader)
         - Google Sheets API (enable this one too — same steps)
    3. Set GOOGLE_SHEET_ID in .env
       Get it from your sheet URL:
       https://docs.google.com/spreadsheets/d/THIS_IS_THE_SHEET_ID/edit
    4. Set GOOGLE_SHEET_TAB to the tab name (default: "Sheet1")

    pip install google-auth google-auth-oauthlib google-api-python-client

Usage:
    from sheets_uploader import append_rows_to_sheet, ensure_header_row

    # Make sure the header row exists (safe to call every run)
    ensure_header_row()

    # Append one or more lead rows
    append_rows_to_sheet([
        {
            "First Name":     "Denis",
            "Last Name":      "Lapalme",
            "Lead Source":    "Succession",
            "Reference Number": "JLR2026031401",
            # ... any subset of COLUMNS
        }
    ])
"""

import os
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

try:
    from googleapiclient.discovery import build
except ImportError:
    raise ImportError("Run: pip install google-auth google-auth-oauthlib google-api-python-client")

# Auth handled centrally — both scopes always together
from google_auth import get_sheets_service as _auth_get_sheets_service, GoogleAuthError
SHEET_ID         = os.getenv("GOOGLE_SHEET_ID", "")
SHEET_TAB        = os.getenv("GOOGLE_SHEET_TAB", "Sheet1")

# Import column definition from excel_uploader so both stay in sync
from excel_uploader import COLUMNS


# ── Auth ──────────────────────────────────────────────────────────────────────

def _get_sheets_service():
    """Delegates to shared google_auth module — always uses both Drive+Sheets scopes."""
    return _auth_get_sheets_service()


# ── Sheet helpers ─────────────────────────────────────────────────────────────

def _get_sheet_values(service, range_: str) -> list:
    """Read a range from the sheet. Returns list of rows (each row is a list)."""
    result = service.spreadsheets().values().get(
        spreadsheetId=SHEET_ID,
        range=range_,
    ).execute()
    return result.get("values", [])


def _get_last_row(service, tab: str) -> int:
    """Return the index of the last row that has data (1-based)."""
    result = service.spreadsheets().values().get(
        spreadsheetId=SHEET_ID,
        range=f"{tab}!A:A",
    ).execute()
    rows = result.get("values", [])
    return len(rows)  # e.g. 50 if 50 rows including header


def _append_values(service, values: list[list]):
    """
    Append rows after the last existing row.
    By anchoring to the row AFTER the last data row (not A1),
    Sheets won't copy the header formatting onto new rows.
    """
    last_row  = _get_last_row(service, SHEET_TAB)
    next_row  = last_row + 1
    start_range = f"{SHEET_TAB}!A{next_row}"

    service.spreadsheets().values().update(
        spreadsheetId=SHEET_ID,
        range=start_range,
        valueInputOption="USER_ENTERED",
        body={"values": values},
    ).execute()


def _update_values(service, range_: str, values: list[list]):
    """Overwrite a specific range (used for header row)."""
    service.spreadsheets().values().update(
        spreadsheetId=SHEET_ID,
        range=range_,
        valueInputOption="USER_ENTERED",
        body={"values": values},
    ).execute()


# ── Public API ────────────────────────────────────────────────────────────────

def ensure_header_row(sheet_id: str = None, tab: str = None): # type: ignore
    """
    Verifies the sheet already has headers. Never writes or modifies them
    so your existing sheet formatting is fully preserved.
    """
    sid = sheet_id or SHEET_ID
    tab = tab or SHEET_TAB
    if not sid:
        raise ValueError("Set GOOGLE_SHEET_ID in .env")

    service  = _get_sheets_service()
    existing = _get_sheet_values(service, f"{tab}!A1:A1")

    if existing and existing[0]:
        first_cell = existing[0][0]
        if first_cell == COLUMNS[0]:
            print(f"  ✅ Sheet header verified ('{first_cell}')")
        else:
            print(f"  ⚠️  Row 1 first cell is '{first_cell}', expected '{COLUMNS[0]}'")
            print(f"      Data will still be appended — check column alignment manually")
    else:
        print(f"  ⚠️  Sheet appears empty — add your headers manually to preserve formatting")


def append_rows_to_sheet(
    rows: list[dict],
    sheet_id: str = None, # type: ignore
    tab: str = None, # type: ignore
) -> int:
    """
    Append lead rows to the Google Sheet.

    Args:
        rows:     list of dicts — keys are column names from COLUMNS.
                  Unknown keys are ignored. Missing keys become empty cells.
        sheet_id: override GOOGLE_SHEET_ID from .env
        tab:      override GOOGLE_SHEET_TAB from .env

    Returns:
        number of rows appended
    """
    sid = sheet_id or SHEET_ID
    tab = tab or SHEET_TAB
    if not sid:
        raise ValueError("Set GOOGLE_SHEET_ID in .env")
    if not rows:
        print("  ⚠️  No rows to append")
        return 0

    service = _get_sheets_service()

    # Convert dicts to ordered lists matching COLUMNS
    values = []
    for row_data in rows:
        values.append([str(row_data.get(col, "") or "") for col in COLUMNS])

    _append_values(service, values)
    print(f"  ✅ Appended {len(rows)} row(s) to Google Sheet '{tab}'")
    return len(rows)


def get_existing_reference_numbers(sheet_id: str = None, tab: str = None) -> set: # type: ignore
    """
    Read the Reference Number column from the sheet and return a set of all
    existing values. Useful for deduplication — skip leads already in the sheet.
    """
    sid = sheet_id or SHEET_ID
    tab = tab or SHEET_TAB
    if not sid:
        return set()

    try:
        ref_col_index = COLUMNS.index("Reference Number")
    except ValueError:
        return set()

    # Convert column index to letter (0=A, 1=B, ...)
    col_letter = chr(ord("A") + ref_col_index)
    service    = _get_sheets_service()
    values     = _get_sheet_values(service, f"{tab}!{col_letter}:{col_letter}")

    existing = set()
    for row in values[1:]:  # skip header
        if row:
            existing.add(row[0])
    print(f"  📋 Found {len(existing)} existing reference numbers in sheet")
    return existing


# ── Test ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    if not SHEET_ID:
        print("❌ GOOGLE_SHEET_ID not set in .env")
        print("   Get it from your sheet URL:")
        print("   https://docs.google.com/spreadsheets/d/THIS_IS_THE_ID/edit")
        exit(1)

    print(f"Testing Google Sheets connection...")
    print(f"  Sheet ID : {SHEET_ID}")
    print(f"  Tab      : {SHEET_TAB}")

    ensure_header_row()

    # Append a test row
    test_row = {col: f"TEST_{col[:8]}" for col in COLUMNS[:5]}
    test_row["Reference Number"] = f"TEST{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
    append_rows_to_sheet([test_row])
    print("✅ Test row appended successfully — check your sheet!")