"""
excel_uploader.py
─────────────────
Writes lead data to an Excel file matching the Google Sheet column structure.

Usage:
    from excel_uploader import write_leads_to_excel

    rows = [
        {
            "Scripte":          "...",
            "Contact Id":       "...",
            "First Name":       "...",
            # ... any columns you want to fill, leave the rest out
        },
        ...
    ]
    write_leads_to_excel(rows, output_path="output/leads.xlsx")
"""

from pathlib import Path
from datetime import datetime
import zoneinfo

TORONTO_TZ = zoneinfo.ZoneInfo("America/Toronto")

try:
    import openpyxl
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
except ImportError:
    raise ImportError("Run: pip install openpyxl")


# ── All column names exactly as in the Google Sheet ──────────────────────────
# Order matters — this is the column order in the Excel file.
# Pass only the keys you want to fill; the rest will be blank.

COLUMNS = [
    "Scripte",
    "Contact Id",
    "Scores",
    "Type",
    "Lead Source",
    "Reference Number",
    "Type Propriete",
    "Price",
    "Verification",
    "Numero lot",
    "Contact Owner.id",
    "Contact Owner",
    "First Name",
    "Last Name",
    "Email",
    "Date of Birth",
    "Company Name.id",
    "Company Name",
    "Mobile",
    "Phone",
    "Other Phone",
    "Home Phone",
    "Email Opt Out",
    "Tag",
    "Description",
    "Created By.id",
    "Created By",
    "Modified By.id",
    "Modified By",
    "Created Time",
    "Modified Time",
    "Contact Name",
    "Last Activity Time",
    "Unsubscribed Mode",
    "Unsubscribed Time",
    "Other Unit",
    "Other Street Number",
    "Other Street",
    "Other City",
    "Other Zip",
    "Other State",
    "Other Country",
    "Mailing Unit",
    "Mailing Street Number",
    "Mailing Street",
    "Mailing City",
    "Mailing Zip",
    "Mailing State",
    "Mailing Country",
    "Source motivation",
    "Equity",
    "Mortgage",
    "Analyse risque",
    "Picture 1",
    "Google Drive",
    "Direct Mail Date",
    "Direct Mail Number",
    "Visite",
    "Date Visite",
    "Call",
]

# ── Lead Source mapping ───────────────────────────────────────────────────────

LEAD_SOURCE_MAP = {
    "Succession":       "Succession",
    "Avis de 60 jours": "60Daysnotice",
    "Vente pour taxes": "Vpti",
}

# ── Reference Number counter ──────────────────────────────────────────────────
# Tracks how many leads have been scraped today within this run.
_ref_counter: dict = {}  # key: "YYYYMMDD" -> count

def _next_ref_number() -> str:
    """
    Generate: JLR + YYYYMMDD + zero-padded 2-digit sequence per day.
    Always uses TODAY's date in Toronto timezone — not the lead's published date.
    Example: JLR2026031401, JLR2026031402 ...
    Counter resets each Toronto calendar day.
    """
    d = datetime.now(TORONTO_TZ).strftime("%Y%m%d")
    _ref_counter[d] = _ref_counter.get(d, 0) + 1
    return f"JLR{d}{_ref_counter[d]:02d}"


# ── Address parsing helpers ───────────────────────────────────────────────────

import re as _re

def _format_postal_code(raw: str) -> str:
    """Uppercase with space after 3rd char: "h2b2j3" or "H2B2J3" -> "H2B 2J3" """
    if not raw:
        return ""
    clean = raw.upper().replace(" ", "")
    if len(clean) == 6:
        return f"{clean[:3]} {clean[3:]}"
    return clean

def _title_case_name(name: str) -> str:
    """
    Clean name capitalisation: "BEAUPRÉ, CHANTAL" or "beaupre, chantal"
    -> "Beaupré, Chantal"
    Works with accented characters.
    """
    if not name:
        return ""
    # Handle "LastName, FirstName" format
    if "," in name:
        parts = name.split(",", 1)
        return ", ".join(p.strip().title() for p in parts)
    return name.strip().title()

def _parse_street_number(street: str) -> tuple[str, str]:
    """
    Split street string into (street_number, street_name).
    Handles:
      "10672 Avenue De Lorimier"   -> ("10672", "Avenue De Lorimier")
      "6630, rue Eugene-Achard"    -> ("6630", "rue Eugene-Achard")   # comma after number
      "rue Eugene-Achard"          -> ("", "rue Eugene-Achard")       # no number
    """
    s = street.strip()
    # Remove leading/trailing commas that sometimes appear: "6630, rue..."
    m = _re.match(r'^(\d+[\w-]*)[,\s]+(.*)', s)
    if m:
        return m.group(1).rstrip(",").strip(), m.group(2).strip()
    return "", s

def _parse_unit_from_street(street: str) -> tuple[str, str]:
    """
    Extract unit prefix if present, return (unit, remaining_street).
    Handles:
      "1110-1110a Chemin..."   -> unit="1110a",  street="1110 Chemin..."
      "101-456 Rue Test"       -> unit="101",    street="456 Rue Test"
      "456 Rue Test, Apt 3"   -> unit="3",       street="456 Rue Test"
      "6630, rue Test"         -> unit="",        street="6630, rue Test"
    """
    s = street.strip()

    # Pattern: "NUM-NUMa? Street" where second part starts with same digits + optional letter
    # e.g. "1110-1110a Chemin" → street_num=1110, unit=1110a
    m = _re.match(r'^(\d+)-(\d+[a-zA-Z]?)\s+(.*)', s)
    if m:
        first_num  = m.group(1)
        second_num = m.group(2)
        rest       = m.group(3).strip()
        # If second part is just digits (different number) it's unit-streetnum format
        if second_num.isdigit():
            return first_num, f"{second_num} {rest}".strip()
        else:
            # second part has a letter suffix — it's a unit variant like "1110a"
            return second_num, f"{first_num} {rest}".strip()

    # Pattern: suffix apt/suite/unit/# keyword
    m2 = _re.search(r'[\s,]+(apt|app|suite|unit|#)\s*(\w+)', s, _re.IGNORECASE)
    if m2:
        return m2.group(2), s[:m2.start()].strip()

    return "", s


def _get_district_value(district_info: list, label: str) -> str:
    """Get value from districtInfo array by label."""
    for item in (district_info or []):
        if item.get("label") == label:
            return item.get("value", "")
    return ""

def _get_district_number(district_info: list, label: str):
    """Get value_number from districtInfo array by label."""
    for item in (district_info or []):
        if item.get("label") == label:
            return item.get("value_number")
    return None

def _clean_price(raw_value: str) -> str:
    """
    "838 100,00" -> "838100"
    Remove spaces, remove decimal fraction (,xx at end).
    """
    import re
    v = raw_value.replace(" ", "").strip()
    # Remove fraction: ,00 or .00 at end
    v = re.sub(r'[,\.]\d+$', "", v)
    # Remove any remaining non-digit chars
    v = re.sub(r'[^0-9]', "", v)
    return v


# ── Data cleaning / mapping ───────────────────────────────────────────────────

def clean_lead(list_doc: dict, detail_doc: dict = None) -> dict: # type: ignore
    """
    Map raw API data to the Excel column structure.

    Args:
        list_doc:   raw document from the list API
        detail_doc: raw document from GET /documents/{id}

    Returns:
        dict with keys matching COLUMNS — pass directly to write_leads_to_excel()
    """
    # Use detail_doc as primary source when available (richer data),
    # fall back to list_doc for any missing fields
    doc = detail_doc if detail_doc else list_doc

    doc_type  = list_doc.get("type", "")
    published = list_doc.get("publishedDate", "")
    try:
        pub_date = datetime.fromisoformat(published.replace("Z", "+00:00"))
    except Exception:
        pub_date = datetime.utcnow()

    # ── Address parsing ───────────────────────────────────────────────
    address_street  = doc.get("addressStreet") or ""
    address_city    = doc.get("addressCity") or ""
    address_zip_raw = doc.get("addressZipCode") or ""
    address_zip     = _format_postal_code(address_zip_raw)

    unit, street_without_unit = _parse_unit_from_street(address_street)
    street_num, street_name   = _parse_street_number(street_without_unit)

    # ── District info ─────────────────────────────────────────────────
    district_info = doc.get("districtInfo", [])
    price_raw     = _get_district_value(district_info, "Valeur de l'immeuble")
    price         = _clean_price(price_raw) if price_raw else ""

    # ── Heir (First Name / Last Name) — from parties array ──────────
    # Priority: Legataire > Debiteur > second party > first party
    parties    = doc.get("parties", [])
    # Also check inside acts array if parties is empty at top level
    if not parties:
        acts = doc.get("acts", [])
        if acts:
            parties = acts[0].get("parties", [])

    heir_person = {}
    if parties:
        # Look for Legataire first, then Debiteur
        for target_name in ("Legataire", "Debiteur"):
            for party in parties:
                if party.get("name", "").strip().lower() == target_name.lower():
                    values = party.get("values", [])
                    if values:
                        heir_person = values[0]
                        break
            if heir_person:
                break

        # Fallback: second party first value, then first party first value
        if not heir_person and len(parties) >= 2:
            values = parties[1].get("values", [])
            if values:
                heir_person = values[0]
        if not heir_person and parties:
            values = parties[0].get("values", [])
            if values:
                heir_person = values[0]

    first_name = _title_case_name(heir_person.get("firstName", ""))
    last_name  = _title_case_name(heir_person.get("lastName", ""))

    # ── Mailing address — from heir_person (Legataire) address ──────
    # Same person as First/Last Name, same priority logic already resolved above
    mailing_street_raw = heir_person.get("addressStreet") or ""
    mailing_city       = heir_person.get("addressCity") or ""
    mailing_zip        = _format_postal_code(heir_person.get("addressZipCode") or "")

    m_unit, m_street_no_unit    = _parse_unit_from_street(mailing_street_raw)
    m_street_num, m_street_name = _parse_street_number(m_street_no_unit)

    # ── Cadastre (lot number) ─────────────────────────────────────────
    numero_lot = doc.get("cadastreNumber") or ""

    row = {
        # Fixed values
        "Type":           "Prospect",
        "Type Propriete": "R-House",

        # Mapped from doc type
        "Lead Source":    LEAD_SOURCE_MAP.get(doc_type, doc_type),

        # JLR + YYYYMMDD + 2-digit sequence  e.g. JLR2026031401
        "Reference Number": _next_ref_number(),

        # Municipal assessment price (cleaned integer string)
        "Price":          price,

        # Cadastral number
        "Numero lot":     numero_lot,

        # Property address — "Other" fields
        "Other Unit":          unit,
        "Other Street Number": street_num,
        "Other Street":        street_name,
        "Other City":          address_city,
        "Other Zip":           address_zip,
        "Other State":         "Quebec",
        "Other Country":       "Canada",

        # Mailing address — from Legataire (same person as First/Last Name)
        "Mailing Unit":          m_unit,
        "Mailing Street Number": m_street_num,
        "Mailing Street":        m_street_name,
        "Mailing City":          mailing_city,
        "Mailing Zip":           mailing_zip,
        "Mailing State":         "Quebec",
        "Mailing Country":       "Canada",

        # Heir name — Legataire > Debiteur > fallback
        "First Name":      first_name,
        "Last Name":       last_name,

        # Source motivation — filled from property_history API (passed in separately)
        "Source motivation": "",   # populated by caller after property_history API call

        # ── Still pending ──
        # "Mobile":       ...,
        # "Equity":       ...,
        # "Mortgage":     ...,
        # "Google Drive": ...,   <- filled by drive_uploader
    }

    return row


# ── Styles ────────────────────────────────────────────────────────────────────

HEADER_FILL = PatternFill("solid", fgColor="1F4E79")
HEADER_FONT = Font(color="FFFFFF", bold=True, size=11)
EVEN_FILL   = PatternFill("solid", fgColor="EEF2FF")
ODD_FILL    = PatternFill("solid", fgColor="FFFFFF")
THIN_BORDER = Border(
    bottom=Side(style="thin", color="CCCCCC"),
    right=Side(style="thin", color="DDDDDD"),
)


# ── Main function ─────────────────────────────────────────────────────────────

def write_leads_to_excel(
    rows: list[dict],
    output_path: str = "output/leads.xlsx",
    sheet_name: str = "Leads",
) -> str:
    """
    Write a list of lead dicts to an Excel file.

    Each dict can contain any subset of the column names defined in COLUMNS.
    Keys not in COLUMNS are silently ignored.
    Columns not present in a row are left blank.

    Args:
        rows:        list of dicts, each dict is one lead row
        output_path: where to save the .xlsx file
        sheet_name:  worksheet name

    Returns:
        absolute path to the saved file
    """
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = sheet_name # type: ignore

    # ── Header row ────────────────────────────────────────────────────
    ws.append(COLUMNS) # type: ignore
    for cell in ws[1]: # type: ignore
        cell.fill      = HEADER_FILL
        cell.font      = HEADER_FONT
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        cell.border    = THIN_BORDER
    ws.row_dimensions[1].height = 32 # type: ignore
    ws.freeze_panes = "A2" # type: ignore

    # ── Data rows ─────────────────────────────────────────────────────
    for row_idx, row_data in enumerate(rows, start=2):
        row_values = [row_data.get(col, "") for col in COLUMNS]
        ws.append(row_values) # type: ignore

        fill = EVEN_FILL if row_idx % 2 == 0 else ODD_FILL
        for cell in ws[row_idx]: # type: ignore
            cell.fill      = fill
            cell.alignment = Alignment(vertical="center", wrap_text=False)
            cell.border    = THIN_BORDER

    # ── Column widths ─────────────────────────────────────────────────
    for col_idx, col_name in enumerate(COLUMNS, start=1):
        col_letter = ws.cell(row=1, column=col_idx).column_letter # type: ignore
        # Width = max of header length vs longest data value, capped at 50
        max_len = len(col_name)
        for row_data in rows:
            val = str(row_data.get(col_name, "") or "")
            if len(val) > max_len:
                max_len = len(val)
        ws.column_dimensions[col_letter].width = min(max_len + 4, 50) # type: ignore

    wb.save(str(path))
    size_kb = path.stat().st_size / 1024
    print(f"✅ Excel saved → {path.resolve()}  ({len(rows)} rows, {size_kb:.1f} KB)")
    return str(path.resolve())


def append_leads_to_excel(
    rows: list[dict],
    output_path: str = "output/leads.xlsx",
    sheet_name: str = "Leads",
) -> str:
    """
    Append rows to an existing Excel file.
    If the file doesn't exist, creates it fresh.
    """
    path = Path(output_path)
    if not path.exists():
        return write_leads_to_excel(rows, output_path, sheet_name)

    wb = openpyxl.load_workbook(str(path))
    ws = wb[sheet_name] if sheet_name in wb.sheetnames else wb.active

    next_row = ws.max_row + 1 # type: ignore
    for row_idx, row_data in enumerate(rows, start=next_row):
        row_values = [row_data.get(col, "") for col in COLUMNS]
        ws.append(row_values) # type: ignore
        fill = EVEN_FILL if row_idx % 2 == 0 else ODD_FILL
        for cell in ws[row_idx]: # type: ignore
            cell.fill      = fill
            cell.alignment = Alignment(vertical="center")
            cell.border    = THIN_BORDER

    wb.save(str(path))
    print(f"✅ Appended {len(rows)} rows → {path.resolve()}")
    return str(path.resolve())


# ── Quick test ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    dummy_rows = [
        {
            "First Name":       "Denis",
            "Last Name":        "Lapalme",
            "Mailing Street":   "10672 Avenue De Lorimier",
            "Mailing City":     "Montreal",
            "Mailing Zip":      "H2B2J3",
            "Mailing State":    "QC",
            "Mailing Country":  "Canada",
            "Type Propriete":   "Unifamilial",
            "Type":             "Succession",
            "Lead Source":      "MonProspecteur",
            "Reference Number": "5eae5b61209a761738f4c776",
            "Google Drive":     "https://drive.google.com/...",
            "Created Time":     datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        },
        {
            "First Name":       "Marie",
            "Last Name":        "Tremblay",
            "Mailing Street":   "456 Rue Sainte-Catherine",
            "Mailing City":     "Montreal",
            "Mailing Zip":      "H3B1A7",
            "Type Propriete":   "Condo",
            "Type":             "Succession",
            "Lead Source":      "MonProspecteur",
            "Equity":           "120000",
            "Mortgage":         "80000",
        },
    ]

    write_leads_to_excel(dummy_rows, "output/test_leads.xlsx")