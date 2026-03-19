"""
run_history.py
──────────────
Maintains a local history of every lead ever scraped, with upload status.
Written to output/data/scrape_history.json

This is the source of truth for retries — since calling the detail API
marks leads as read on the site, this file is the only way to know:
  - Which leads were scraped
  - Whether they were uploaded to Drive / Sheet
  - What failed and why

Structure of each entry:
{
    "doc_id":          "5eae5b61209a761738f4c776",
    "reference_number": "JLR2026031401",
    "address":         "10672 Avenue De Lorimier, Montreal",
    "lead_source":     "Succession",
    "scraped_at":      "2026-03-14T14:59:42Z",
    "run_id":          "20260314_145942",

    "act_pdf":         "output/pdfs/5eae5b61209a761738f4c776.pdf",
    "print_pdf":       "output/prints/5eae5b61209a761738f4c776_print.pdf",

    "scrape_ok":       true,
    "scrape_error":    null,

    "drive_ok":        true,
    "drive_url":       "https://drive.google.com/drive/folders/...",
    "drive_error":     null,
    "drive_attempts":  1,

    "sheet_ok":        true,
    "sheet_error":     null,

    "excel_ok":        true,
    "excel_path":      "output/data/leads_20260314_145942.xlsx",

    "needs_retry":     false   // true if any upload step failed
}
"""

import json
from pathlib import Path
from datetime import datetime, timezone

HISTORY_FILE = Path("output/data/scrape_history.json")


def _load() -> list:
    if not HISTORY_FILE.exists():
        return []
    try:
        return json.loads(HISTORY_FILE.read_text())
    except Exception:
        return []


def _save(history: list):
    HISTORY_FILE.parent.mkdir(parents=True, exist_ok=True)
    HISTORY_FILE.write_text(json.dumps(history, indent=2, ensure_ascii=False, default=str))


def upsert_lead(entry: dict):
    """
    Insert or update a lead entry by doc_id.
    If the doc_id already exists, merge the new fields into the existing entry.
    """
    history = _load()
    doc_id  = entry.get("doc_id")

    existing_idx = next((i for i, e in enumerate(history) if e.get("doc_id") == doc_id), None)

    if existing_idx is not None:
        history[existing_idx].update(entry)
    else:
        history.append(entry)

    _save(history)


def record_scrape(
    doc_id:     str,
    address:    str,
    lead_source: str,
    run_id:     str,
    act_pdf:    str = None,
    print_pdf:  str = None,
    scrape_ok:  bool = True,
    scrape_error: str = None,
    reference_number: str = "",
) -> dict:
    """Call this as soon as a lead is scraped (before upload attempts)."""
    entry = {
        "doc_id":           doc_id,
        "reference_number": reference_number,
        "address":          address,
        "lead_source":      lead_source,
        "scraped_at":       datetime.now(timezone.utc).isoformat(),
        "run_id":           run_id,
        "act_pdf":          act_pdf,
        "print_pdf":        print_pdf,
        "scrape_ok":        scrape_ok,
        "scrape_error":     scrape_error,
        # Upload fields default to pending
        "drive_ok":         None,
        "drive_url":        "",
        "drive_error":      None,
        "drive_attempts":   0,
        "sheet_ok":         None,
        "sheet_error":      None,
        "excel_ok":         None,
        "excel_path":       "",
        "needs_retry":      not scrape_ok,
    }
    upsert_lead(entry)
    return entry


def record_drive_result(doc_id: str, ok: bool, url: str = "", error: str = None, attempts: int = 1):
    """Call after Drive upload attempt(s) for a lead."""
    upsert_lead({
        "doc_id":        doc_id,
        "drive_ok":      ok,
        "drive_url":     url,
        "drive_error":   error,
        "drive_attempts": attempts,
        "needs_retry":   not ok,
    })


def record_sheet_result(doc_ids: list, ok: bool, error: str = None):
    """Call after Sheet upload attempt. Applies to all doc_ids in the batch."""
    for doc_id in doc_ids:
        entry = {"doc_id": doc_id, "sheet_ok": ok, "sheet_error": error}
        # needs_retry = True if sheet failed OR drive previously failed
        history = _load()
        existing = next((e for e in history if e.get("doc_id") == doc_id), {})
        drive_ok = existing.get("drive_ok", True)
        entry["needs_retry"] = not ok or not drive_ok
        upsert_lead(entry)


def record_excel_result(doc_ids: list, ok: bool, path: str = "", error: str = None):
    """Call after Excel write."""
    for doc_id in doc_ids:
        upsert_lead({"doc_id": doc_id, "excel_ok": ok, "excel_path": path, "excel_error": error})


def get_pending_retries() -> list:
    """Return all entries where needs_retry=True, ordered oldest first."""
    history = _load()
    return [e for e in history if e.get("needs_retry")]


def get_summary(run_id: str = None) -> dict:
    """Return counts for a specific run or all runs."""
    history = _load()
    if run_id:
        history = [e for e in history if e.get("run_id") == run_id]

    return {
        "total":          len(history),
        "scrape_ok":      sum(1 for e in history if e.get("scrape_ok")),
        "scrape_failed":  sum(1 for e in history if not e.get("scrape_ok")),
        "drive_ok":       sum(1 for e in history if e.get("drive_ok")),
        "drive_failed":   sum(1 for e in history if e.get("drive_ok") is False),
        "drive_pending":  sum(1 for e in history if e.get("drive_ok") is None),
        "sheet_ok":       sum(1 for e in history if e.get("sheet_ok")),
        "sheet_failed":   sum(1 for e in history if e.get("sheet_ok") is False),
        "needs_retry":    sum(1 for e in history if e.get("needs_retry")),
    }


if __name__ == "__main__":
    print(f"History file: {HISTORY_FILE.resolve()}")
    pending = get_pending_retries()
    if pending:
        print(f"\n⚠️  {len(pending)} leads need retry:")
        for e in pending:
            drive  = "✅" if e.get("drive_ok") else ("❌" if e.get("drive_ok") is False else "⏳")
            sheet  = "✅" if e.get("sheet_ok") else ("❌" if e.get("sheet_ok") is False else "⏳")
            print(f"  [{e['run_id']}] {e['address']}  drive={drive}  sheet={sheet}")
    else:
        print("✅ No pending retries")
    print(f"\nOverall summary: {json.dumps(get_summary(), indent=2)}")