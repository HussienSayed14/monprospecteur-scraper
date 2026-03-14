"""
email_sender.py
───────────────
Standalone module. Sends summary emails via Gmail SMTP.
Uses the same Gmail account already configured for OTP.

Setup:
    Same GMAIL_USER + GMAIL_APP_PASSWORD from your .env — no extra setup needed.

Usage:
    from email_sender import send_summary_email

    send_summary_email(
        stats_summary = stats.summary,          # dict from RunStats.summary
        to            = ["you@email.com"],
        excel_path    = "output/leads.xlsx",    # optional attachment
    )
"""

import os
import json
import smtplib
import zoneinfo

TORONTO_TZ = zoneinfo.ZoneInfo("America/Toronto")
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

GMAIL_USER         = os.getenv("GMAIL_USER")
GMAIL_APP_PASSWORD = os.getenv("GMAIL_APP_PASSWORD")
DEFAULT_TO         = os.getenv("SUMMARY_EMAIL_TO", GMAIL_USER)  # default: send to yourself


# ── HTML email builder ────────────────────────────────────────────────────────

def _fmt_toronto(iso_str: str) -> str:
    """Convert ISO UTC string to Toronto local time string."""
    if not iso_str:
        return "N/A"
    try:
        from datetime import datetime, timezone
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        dt_toronto = dt.astimezone(TORONTO_TZ)
        return dt_toronto.strftime("%Y-%m-%d %H:%M:%S %Z")
    except Exception:
        return iso_str


def build_html_body(stats: dict, sheet_ok: bool = True, sheet_log: list = None) -> str: # type: ignore
    succeeded = stats.get("succeeded", [])
    failed    = stats.get("failed", [])

    success_rows = ""
    for s in succeeded:
        drive_act   = f'<a href="{s["drive_act_url"]}">Act PDF</a>'   if s.get("drive_act_url")   else "—"
        drive_print = f'<a href="{s["drive_print_url"]}">Print PDF</a>' if s.get("drive_print_url") else "—"
        success_rows += f"""
        <tr>
            <td>{s.get('address','')}</td>
            <td>{s.get('id','')}</td>
            <td>{drive_act}</td>
            <td>{drive_print}</td>
        </tr>"""

    failed_rows = ""
    for f in failed:
        failed_rows += f"""
        <tr style="background:#fff3f3">
            <td>{f.get('address','')}</td>
            <td>{f.get('id','')}</td>
            <td style="color:#c00">{f.get('step','')}</td>
            <td style="color:#c00">{f.get('error','')}</td>
        </tr>"""

    duration = stats.get("duration_seconds", 0)
    duration_str = f"{duration // 60}m {duration % 60}s"

    has_failures = len(failed) > 0
    status_color = "#c00" if has_failures else "#2e7d32"
    status_label = f"⚠️ {len(failed)} failures" if has_failures else "✅ All succeeded"

    # Build per-lead upload status rows
    upload_rows = ""
    for s in succeeded:
        drive_ok  = s.get("drive_upload_ok", True)
        drive_log = s.get("drive_upload_log", [])
        drive_icon = "✅" if drive_ok else "❌"
        drive_attempts = len(drive_log) + 1 if drive_log else 1
        drive_cell = f'{drive_icon} ({drive_attempts} attempt{"s" if drive_attempts > 1 else ""})' 
        if drive_log:
            drive_cell += "<br><small style='color:#888'>" + "<br>".join(drive_log[-2:]) + "</small>"

        drive_url = s.get("drive_url", "")
        drive_link = f'<a href="{drive_url}">Open</a>' if drive_url else "—"

        upload_rows += f"""
        <tr>
            <td>{s.get("address","")}</td>
            <td style="text-align:center">{drive_cell}</td>
            <td style="text-align:center">{drive_link}</td>
        </tr>"""

    sheet_status_cell = "✅ Uploaded" if sheet_ok else (
        "❌ Failed after 5 attempts<br><small style='color:#888'>" +
        "<br>".join((sheet_log or [])[-2:]) + "</small>"
    )

    has_upload_issues = any(not s.get("drive_upload_ok", True) for s in succeeded) or not sheet_ok
    overall_ok = not has_failures and not has_upload_issues

    # Detect auth errors from logs
    auth_error = any(
        "Auth error" in " ".join(s.get("drive_upload_log", [])) for s in succeeded
    ) or any("Auth error" in l for l in (sheet_log or []))

    auth_banner = """<div style='background:#fff0f0;border:2px solid #c00;padding:16px;border-radius:8px;margin-bottom:20px'>
<h3 style='color:#c00;margin:0 0 8px 0'>&#x1F510; Google Auth Error</h3>
<p style='margin:0'>The Google token expired or is missing required scopes.<br>
<b>Action required:</b> Delete <code>token.json</code> and run <code>python sheets_uploader.py</code>
to re-authenticate, then run <code>python main.py --retry-uploads</code></p>
</div>""" if auth_error else ""

    return f"""
<!DOCTYPE html>
<html>
<head>
<style>
  body        {{ font-family: Arial, sans-serif; font-size: 14px; color: #333; }}
  h2          {{ color: #1a237e; }}
  .stats      {{ background: #f5f5f5; padding: 16px; border-radius: 8px; margin-bottom: 20px; }}
  .stats td   {{ padding: 4px 16px 4px 0; }}
  .status     {{ font-weight: bold; color: {status_color}; font-size: 16px; }}
  table.data  {{ border-collapse: collapse; width: 100%; margin-top: 12px; }}
  table.data th {{ background: #1a237e; color: white; padding: 8px 12px; text-align: left; }}
  table.data td {{ border-bottom: 1px solid #ddd; padding: 7px 12px; vertical-align: top; }}
  table.data tr:hover td {{ background: #f0f4ff; }}
  h3          {{ margin-top: 28px; color: #1a237e; border-bottom: 2px solid #1a237e; padding-bottom: 4px; }}
  .ok         {{ color: #2e7d32; font-weight: bold; }}
  .fail       {{ color: #c00; font-weight: bold; }}
</style>
</head>
<body>

<h2>🏠 MonProspecteur — Scraper Run Summary</h2>

{auth_banner}

<div class="stats">
  <table>
    <tr><td>Run date</td>       <td><b>{_fmt_toronto(stats.get('run_started_at',''))}</b></td></tr>
    <tr><td>Duration</td>       <td>{duration_str}</td></tr>
    <tr><td>Status</td>         <td class="status">{status_label}</td></tr>
    <tr><td>Total fetched</td>  <td>{stats.get('total_fetched', 0)}</td></tr>
    <tr><td>Unread leads</td>   <td>{stats.get('total_unread', 0)}</td></tr>
    <tr><td>Skipped (read)</td> <td>{stats.get('total_skipped_read', 0)}</td></tr>
    <tr><td>✅ Scraped OK</td>  <td><b>{stats.get('succeeded_count', 0)}</b></td></tr>
    <tr><td>❌ Scrape failed</td><td><b class="fail">{stats.get('failed_count', 0)}</b></td></tr>
    <tr><td>Google Sheet</td>   <td>{"<span class='ok'>✅ Uploaded</span>" if sheet_ok else "<span class='fail'>❌ Failed (5 attempts)</span>"}</td></tr>
  </table>
</div>

{'<h3>📋 Upload Status Per Lead</h3><table class="data"><thead><tr><th>Address</th><th>Drive Upload</th><th>Drive Link</th></tr></thead><tbody>' + upload_rows + '</tbody></table>' if succeeded else ''}

{'<h3>❌ Scrape Failures</h3><table class="data"><thead><tr><th>Address</th><th>ID</th><th>Step</th><th>Error</th></tr></thead><tbody>' + failed_rows + '</tbody></table>' if failed else ''}

{'<h3>⚠️ Sheet Upload Log</h3><pre style="background:#fff3f3;padding:12px;font-size:12px;">' + chr(10).join(sheet_log or []) + '</pre>' if not sheet_ok else ''}

<p style="margin-top:32px; color:#888; font-size:12px;">
  Sent automatically by MonProspecteur scraper.
  {'<b>Retry scrape failures: python main.py --retry</b><br>' if has_failures else ''}
  {'<b>Retry upload failures: python main.py --retry-uploads</b>' if has_upload_issues else ''}
</p>

</body>
</html>
"""


# ── Send email ────────────────────────────────────────────────────────────────

def send_summary_email(
    stats_summary:  dict,
    to:             list[str] = None, # type: ignore
    excel_path:     str = None, # type: ignore
    extra_attachments: list[str] = None, # type: ignore
    sheet_ok:       bool = True,
    sheet_log:      list = None, # type: ignore
):
    """
    Send the run summary email.

    Args:
        stats_summary:      dict from RunStats.summary
        to:                 list of recipient email addresses
                            (defaults to SUMMARY_EMAIL_TO env var or GMAIL_USER)
        excel_path:         optional path to leads Excel file to attach
        extra_attachments:  optional list of extra file paths to attach
    """
    recipients = to or ([DEFAULT_TO] if DEFAULT_TO else [])
    if not recipients:
        print("⚠️  No recipients configured — skipping email")
        return

    succeeded_count = stats_summary.get("succeeded_count", 0)
    failed_count    = stats_summary.get("failed_count", 0)
    run_date        = stats_summary.get("run_started_at", "")[:10]

    # Count upload-level issues across succeeded docs
    drive_retried  = [s for s in stats_summary.get("succeeded", []) if s.get("drive_upload_log")]
    drive_failed   = [s for s in stats_summary.get("succeeded", []) if not s.get("drive_upload_ok", True)]
    upload_issues  = len(drive_failed) + (0 if (sheet_ok if sheet_ok is not None else True) else 1)

    # Detect auth errors specifically
    auth_error = any(
        "Auth error" in " ".join(s.get("drive_upload_log", [])) for s in stats_summary.get("succeeded", [])
    ) or (not (sheet_ok if sheet_ok is not None else True) and sheet_log and any("Auth error" in l for l in (sheet_log or [])))

    subject = (
        f"[MonProspecteur] {run_date} — "
        f"{succeeded_count} leads scraped"
        + (f", ⚠️ {failed_count} scrape failures" if failed_count else "")
        + (f", 🔐 AUTH ERROR" if auth_error else (f", ⚠️ {upload_issues} upload failures" if upload_issues else ""))
        + (" ✅" if not failed_count and not upload_issues else "")
    )

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = GMAIL_USER # type: ignore
    msg["To"]      = ", ".join(recipients)

    # Plain text fallback
    plain = (
        f"MonProspecteur Scraper Run — {run_date}\n"
        f"Succeeded: {succeeded_count}\n"
        f"Failed: {failed_count}\n"
        f"Total unread: {stats_summary.get('total_unread', 0)}\n"
    )
    msg.attach(MIMEText(plain, "plain"))
    msg.attach(MIMEText(build_html_body(stats_summary, sheet_ok=sheet_ok, sheet_log=sheet_log or []), "html"))

    # Attachments
    attachments = []
    if excel_path and Path(excel_path).exists():
        attachments.append(excel_path)
    if extra_attachments:
        attachments.extend(extra_attachments)

    for file_path in attachments:
        path = Path(file_path)
        if not path.exists():
            print(f"  ⚠️  Attachment not found: {file_path} — skipping")
            continue
        part = MIMEBase("application", "octet-stream")
        part.set_payload(path.read_bytes())
        encoders.encode_base64(part)
        part.add_header("Content-Disposition", f'attachment; filename="{path.name}"')
        msg.attach(part)

    # Send
    print(f"\n📧 Sending summary email to {recipients}...")
    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(GMAIL_USER, GMAIL_APP_PASSWORD) # type: ignore
            server.sendmail(GMAIL_USER, recipients, msg.as_string()) # type: ignore
        print(f"  ✅ Email sent: {subject}")
    except Exception as e:
        print(f"  ❌ Email failed: {e}")


if __name__ == "__main__":
    # Quick test with dummy stats
    dummy_stats = {
        "run_started_at":     "2026-03-12T08:00:00+00:00",
        "run_finished_at":    "2026-03-12T08:04:30+00:00",
        "duration_seconds":   270,
        "total_fetched":      45,
        "total_unread":       3,
        "total_skipped_read": 42,
        "succeeded_count":    2,
        "failed_count":       1,
        "succeeded": [
            {"id": "abc123", "address": "123 Rue Test, Montreal", "drive_act_url": "https://drive.google.com/...", "drive_print_url": "https://drive.google.com/..."},
        ],
        "failed": [
            {"id": "def456", "address": "456 Rue Example, Laval", "step": "act_pdf", "error": "404 Not Found"},
        ],
    }
    send_summary_email(
        stats_summary = dummy_stats,
        to            = [GMAIL_USER], # type: ignore
    )