import imaplib
import email
import re
import time
import random
import os
import json
import requests
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from playwright.sync_api import sync_playwright
from dotenv import load_dotenv

load_dotenv()

GMAIL_USER         = os.getenv("GMAIL_USER")
GMAIL_APP_PASSWORD = os.getenv("GMAIL_APP_PASSWORD")
SITE_EMAIL         = os.getenv("SITE_EMAIL")
SITE_PASSWORD      = os.getenv("SITE_PASSWORD")
OTP_SUBJECT_PREFIX = "Forwarded SMS From:"  # sender domain changes (mailer1/2/3/4...) but subject is always consistent

# Webshare proxy — set these in .env
# Format: 82.23.96.252:7478  (host:port only, credentials separate)
PROXY_HOST = os.getenv("PROXY_HOST", "")
PROXY_PORT = os.getenv("PROXY_PORT", "")
PROXY_USER = os.getenv("PROXY_USER", "")
PROXY_PASS = os.getenv("PROXY_PASS", "")

def get_proxy_url() -> str | None:
    """Build proxy URL from parts, or return None if not configured."""
    if not PROXY_HOST or not PROXY_PORT:
        return None
    if PROXY_USER and PROXY_PASS:
        # URL-encode credentials to handle special characters (@ # ! etc.)
        from urllib.parse import quote
        user = quote(PROXY_USER, safe="")
        pwd  = quote(PROXY_PASS, safe="")
        return f"http://{user}:{pwd}@{PROXY_HOST}:{PROXY_PORT}"
    return f"http://{PROXY_HOST}:{PROXY_PORT}"

def get_requests_proxies() -> dict | None:
    """Return proxies dict for requests library, or None if not configured."""
    url = get_proxy_url()
    if not url:
        return None
    return {"http": url, "https": url}
SESSION_FILE  = os.getenv("SESSION_FILE", "session.json")
WEBHOOK_URL   = os.getenv("WEBHOOK_URL", "").strip()  # optional GET call after each lead
DOCUMENTS_URL      = "https://app.monprospecteur.com/app.html#/documents"
LOGIN_URL          = "https://app.monprospecteur.com/auth.html#/connexion"

API_BASE           = "https://api.monprospecteur.com"
API_DOCUMENTS_URL  = f"{API_BASE}/documents"
API_DOCUMENTS_PARAMS = {
    "isFirstSearch":    "true",
    "isMapSearch":      "false",
    "keywords":         "",
    "order_by":         "publishedDate",
    "reverse_order_by": "true",
    "selectedCity":     "",
    "selectedList":     "",
    "selectedSortDate": "",
}

OUTPUT_DIR  = Path("output")
PDFS_DIR    = OUTPUT_DIR / "pdfs"
PRINTS_DIR  = OUTPUT_DIR / "prints"
DATA_DIR    = OUTPUT_DIR / "data"
FAILED_DIR  = OUTPUT_DIR / "failed"
for d in [PDFS_DIR, PRINTS_DIR, DATA_DIR, FAILED_DIR]:
    d.mkdir(parents=True, exist_ok=True)


# ─────────────────────────────────────────────
# STATS TRACKER
# Collects everything needed for the summary email
# ─────────────────────────────────────────────

@dataclass
class RunStats:
    run_started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    run_finished_at: datetime = None # type: ignore

    total_fetched: int = 0          # all docs from API
    total_unread: int = 0           # docs with isRead == False
    total_skipped_read: int = 0     # docs skipped because already read

    # Per-doc results — each entry: { id, address, act_pdf, print_pdf, detail, excel, drive, error }
    succeeded: list = field(default_factory=list)
    failed: list    = field(default_factory=list)

    @property
    def duration_seconds(self):
        if self.run_finished_at:
            return (self.run_finished_at - self.run_started_at).total_seconds()
        return 0

    @property
    def summary(self) -> dict:
        return {
            "run_started_at":    self.run_started_at.isoformat(),
            "run_finished_at":   self.run_finished_at.isoformat() if self.run_finished_at else None,
            "duration_seconds":  round(self.duration_seconds),
            "total_fetched":     self.total_fetched,
            "total_unread":      self.total_unread,
            "total_skipped_read": self.total_skipped_read,
            "succeeded_count":   len(self.succeeded),
            "failed_count":      len(self.failed),
            "succeeded":         self.succeeded,
            "failed":            self.failed,
        }

    def record_success(self, doc_id, address, act_pdf, print_pdf):
        self.succeeded.append({
            "id":        doc_id,
            "address":   address,
            "act_pdf":   act_pdf,
            "print_pdf": print_pdf,
            # excel / drive fields added later by their respective modules
            "excel_uploaded":  None,
            "drive_uploaded":  None,
        })

    def record_failure(self, doc_id, address, step, error):
        self.failed.append({
            "id":      doc_id,
            "address": address,
            "step":    step,   # which step failed: "detail_api" | "act_pdf" | "print_pdf"
            "error":   str(error),
        })

    def print_summary(self):
        print(f"\n{'=' * 60}")
        print("📊 RUN SUMMARY")
        print(f"{'=' * 60}")
        print(f"  Started       : {self.run_started_at.strftime('%Y-%m-%d %H:%M:%S UTC')}")
        print(f"  Finished      : {self.run_finished_at.strftime('%Y-%m-%d %H:%M:%S UTC') if self.run_finished_at else 'N/A'}")
        print(f"  Duration      : {round(self.duration_seconds)}s")
        print(f"  Total fetched : {self.total_fetched}")
        print(f"  Unread leads  : {self.total_unread}")
        print(f"  Skipped (read): {self.total_skipped_read}")
        print(f"  ✅ Succeeded  : {len(self.succeeded)}")
        print(f"  ❌ Failed     : {len(self.failed)}")
        if self.failed:
            print(f"\n  Failed details:")
            for f in self.failed:
                print(f"    • [{f['step']}] {f['address']} ({f['id']}) — {f['error']}")
        print(f"{'=' * 60}")


# ─────────────────────────────────────────────
# HUMAN-LIKE DELAYS
# ─────────────────────────────────────────────

def human_delay(min_s: float = 2.0, max_s: float = 5.0):
    """Random delay between actions to mimic human behavior."""
    t = random.uniform(min_s, max_s)
    print(f"  ⏱  Waiting {t:.1f}s...")
    time.sleep(t)

def between_docs_delay(log=None):
    """Longer pause between processing each document to mimic human behavior."""
    t = random.uniform(30.0, 40.0)
    msg = f"Pausing {t:.1f}s before next document"
    print(f"\n  ⏱  {msg}")
    if log:
        log.info(msg)
    time.sleep(t)


# ─────────────────────────────────────────────
# LOGIN / SESSION
# ─────────────────────────────────────────────

def get_otp_from_gmail(sent_after: datetime, wait=20, retries=6):
    """
    Poll Gmail for the OTP email.
    - Waits `wait` seconds before each attempt
    - Retries up to `retries` times (default: 6 × 20s = 2 minutes total)
    - Searches ALL emails from the sender (not just unread) to avoid
      missing emails that Gmail auto-marks as read
    - Accepts emails sent up to 60s BEFORE sent_after to handle clock skew
    """
    from datetime import timedelta
    # Widen the time window slightly to handle clock skew between
    # the scraper machine and Gmail's server timestamps
    effective_after = sent_after - timedelta(seconds=60)

    for attempt in range(retries):
        print(f"⏳ Waiting {wait}s for OTP (attempt {attempt+1}/{retries})...")
        time.sleep(wait)
        try:
            mail = imaplib.IMAP4_SSL("imap.gmail.com")
            mail.login(GMAIL_USER, GMAIL_APP_PASSWORD) # type: ignore
            mail.select("inbox")

            # Search by subject — sender domain changes (mailer1/2/3/4)
            # but subject always starts with "Forwarded SMS From:"
            _, data = mail.search(None, f'(SUBJECT "{OTP_SUBJECT_PREFIX}")')
            ids = data[0].split()
            print(f"  📬 Found {len(ids)} email(s) with subject '{OTP_SUBJECT_PREFIX}'")

            if not ids:
                mail.logout()
                continue

            # Check most recent emails first
            for email_id in reversed(ids):
                _, msg_data = mail.fetch(email_id, "(RFC822)")
                msg = email.message_from_bytes(msg_data[0][1]) # type: ignore

                date_str = msg.get("Date")
                print(f"  📧 Checking email dated: {date_str}")

                try:
                    email_time = email.utils.parsedate_to_datetime(date_str) # type: ignore
                    if email_time.tzinfo is None:
                        email_time = email_time.replace(tzinfo=timezone.utc)
                except Exception:
                    print(f"  ⚠️  Could not parse date: {date_str} — checking body anyway")
                    email_time = sent_after  # assume it's valid if date unparseable

                if email_time >= effective_after.astimezone(timezone.utc):
                    # Extract body
                    body = ""
                    if msg.is_multipart():
                        for part in msg.walk():
                            ct = part.get_content_type()
                            if ct == "text/plain":
                                try:
                                    body = part.get_payload(decode=True).decode("utf-8", errors="ignore") # type: ignore
                                    break
                                except Exception:
                                    pass
                            elif ct == "text/html" and not body:
                                try:
                                    body = part.get_payload(decode=True).decode("utf-8", errors="ignore") # type: ignore
                                except Exception:
                                    pass
                    else:
                        try:
                            body = msg.get_payload(decode=True).decode("utf-8", errors="ignore") # type: ignore
                        except Exception:
                            body = str(msg.get_payload())

                    print(f"  📝 Body preview: {body[:120].strip()}")

                    otp = re.search(r'\b(\d{6})\b', body)
                    if otp:
                        print(f"✅ OTP found: {otp.group(1)}")
                        mail.logout()
                        return otp.group(1)
                    else:
                        print(f"  ⚠️  Email found but no 6-digit code in body")
                else:
                    print(f"  ⏭  Email too old — skipping")

            mail.logout()

        except Exception as e:
            print(f"  ❌ IMAP error on attempt {attempt+1}: {e}")
            try:
                mail.logout()
            except Exception:
                pass

    raise Exception(f"No OTP found after {retries} attempts ({retries * wait}s total)")


def is_session_valid(page) -> bool:
    print("🔍 Checking session...")
    try:
        page.goto(DOCUMENTS_URL, timeout=60000, wait_until="domcontentloaded")
        page.wait_for_load_state("networkidle", timeout=60000)
    except Exception as e:
        print(f"❌ Session check failed: {e}")
        return False
    if "auth.html" in page.url or "connexion" in page.url or page.query_selector('input[name="userid"]'):
        print("❌ Session expired")
        return False
    print("✅ Session valid")
    return True


def login(page, context):
    print("🌐 Logging in...")
    page.goto(LOGIN_URL, timeout=60000, wait_until="domcontentloaded")
    page.wait_for_load_state("networkidle", timeout=60000)
    page.click('input[name="userid"]')
    page.wait_for_timeout(500)
    page.type('input[name="userid"]', SITE_EMAIL, delay=80)
    page.click('input[name="password"]')
    page.wait_for_timeout(400)
    page.type('input[name="password"]', SITE_PASSWORD, delay=100)
    page.wait_for_timeout(2000)
    page.hover('button[type="submit"]')
    page.wait_for_timeout(300)
    page.click('button[type="submit"]')
    page.wait_for_load_state("networkidle", timeout=60000)
    page.wait_for_selector('button[type="submit"]', timeout=80000)
    page.hover('button[type="submit"]')
    page.wait_for_timeout(300)
    otp_requested_at = datetime.now(timezone.utc)
    page.click('button[type="submit"]')
    otp = get_otp_from_gmail(sent_after=otp_requested_at, wait=30)
    page.wait_for_selector('#confirmationCodeInput', timeout=60000)
    page.click('#confirmationCodeInput')
    page.wait_for_timeout(300)
    page.type('#confirmationCodeInput', otp, delay=150)
    page.wait_for_timeout(500)
    page.click('button[ng-click="confirmTwoFactorCode ()"]')
    page.wait_for_load_state("networkidle", timeout=60000)
    context.storage_state(path=SESSION_FILE)
    print(f"💾 Session saved → {SESSION_FILE}")


# ─────────────────────────────────────────────
# REQUESTS SESSION FROM PLAYWRIGHT COOKIES
# ─────────────────────────────────────────────

def build_requests_session(storage_state: dict) -> requests.Session:
    session = requests.Session()
    session.headers.update({
        "User-Agent":      "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept":          "application/json, text/plain, */*",
        "Accept-Language": "fr-CA,fr;q=0.9,en-CA;q=0.8,en;q=0.7",
        "Origin":          "https://app.monprospecteur.com",
        "Referer":         "https://app.monprospecteur.com/",
        "sec-fetch-dest":  "empty",
        "sec-fetch-mode":  "cors",
        "sec-fetch-site":  "same-site",
    })
    proxies = get_requests_proxies()
    if proxies:
        session.proxies.update(proxies)
        print(f"  🌐 Requests proxy: {PROXY_HOST}:{PROXY_PORT}")
    for cookie in storage_state.get("cookies", []):
        session.cookies.set(
            cookie["name"], cookie["value"],
            domain=cookie.get("domain", ""),
            path=cookie.get("path", "/"),
        )
    return session


# ─────────────────────────────────────────────
# STEP 1 — Fetch all documents, filter unread
# ─────────────────────────────────────────────

def fetch_all_documents(session: requests.Session, log=None) -> list:
    print("\n" + "=" * 60)
    print("📡 FETCHING ALL DOCUMENTS")
    print("=" * 60)

    all_docs = []
    page_num = 0

    while True:
        params = {**API_DOCUMENTS_PARAMS, "page": page_num}
        resp = session.get(API_DOCUMENTS_URL, params=params, timeout=30)
        msg = f"Fetching page {page_num} — status {resp.status_code}"
        print(msg)
        if log: log.info(msg)

        if resp.status_code != 200:
            print(f"❌ {resp.text[:300]}")
            break

        data = resp.json()

        documents = data if isinstance(data, list) else (
            data.get("documents") or data.get("data") or
            data.get("results")   or data.get("items") or []
        )

        if not documents:
            print(f"  No more docs at page {page_num} — done")
            break

        all_docs.extend(documents)
        print(f"  +{len(documents)} docs  (total: {len(all_docs)})")

        if len(documents) < 10:
            break
        page_num += 1
        t = random.uniform(3.0, 6.0)
        log.info(f'Page {page_num} fetched — waiting {t:.1f}s before next page') # type: ignore
        time.sleep(t)

    msg = f"Total documents fetched: {len(all_docs)}"
    print(f"\n✅ {msg}")
    if log: log.ok(msg)
    return all_docs


def filter_unread(all_docs: list, stats: RunStats) -> list:
    """Keep only docs where isRead == False."""
    stats.total_fetched = len(all_docs)
    unread = [d for d in all_docs if not d.get("isRead", True)]
    stats.total_unread = len(unread)
    stats.total_skipped_read = len(all_docs) - len(unread)
    print(f"\n🔍 Filter: {len(all_docs)} total → {len(unread)} unread, {stats.total_skipped_read} skipped (already read)")
    return unread


# ─────────────────────────────────────────────
# STEP 2a — Detail API
# GET /documents/{doc_id}
# ─────────────────────────────────────────────

def fetch_document_details(doc_id: str, session: requests.Session) -> dict | None:
    url = f"{API_BASE}/documents/{doc_id}"
    print(f"  → GET {url}")
    resp = session.get(url, timeout=20)
    print(f"    Status: {resp.status_code}")
    if resp.status_code == 200:
        return resp.json()
    print(f"    ❌ {resp.text[:300]}")
    return None


# ─────────────────────────────────────────────
# STEP 2a-2 — Property History API
# GET /documents/{doc_id}/property_history
# Used to populate "Source motivation" column
# ─────────────────────────────────────────────

SOURCE_MOTIVATION_MAP = {
    "Vente sous contrôle de justice":                          "60Daynotice",
    "Préavis de vente pour défaut de paiement impôt foncier": "VPTI",
    "Déclaration de transmission":                             "Succession",
}

def fetch_property_history(doc_id: str, session: requests.Session, log=None) -> str:
    """
    GET /documents/{doc_id}/property_history
    Reads IndexImmeubles[].NatureActe and maps to Source motivation values.
    Returns comma-separated string of matched motivations, or "".
    """
    url = f"{API_BASE}/documents/{doc_id}/property_history"
    if log:
        log.info("Fetching property history", url=url)
    try:
        resp = session.get(url, timeout=20)
        if resp.status_code != 200:
            if log:
                log.warn("Property history not available", status=resp.status_code, doc=doc_id)
            return ""
        data = resp.json()
        index = data.get("IndexImmeubles", [])
        found = []
        for entry in index:
            nature = entry.get("NatureActe", "").strip()
            mapped = SOURCE_MOTIVATION_MAP.get(nature)
            if mapped and mapped not in found:
                found.append(mapped)
        result = ", ".join(found)
        if log:
            log.ok(f"Source motivation: {result or 'none'}", doc=doc_id)
        return result
    except Exception as e:
        if log:
            log.error("Property history fetch failed", error=str(e), doc=doc_id)
        return ""


# ─────────────────────────────────────────────
# STEP 2b — File 1: Act PDF
# GET /acts/{act_id}/act.pdf → output/pdfs/{doc_id}.pdf
# ─────────────────────────────────────────────

def download_act_pdf(doc_id: str, act_id: str, session: requests.Session) -> str | None:
    if not act_id:
        print("  ⚠️  No act_id — skipping")
        return None

    url  = f"{API_BASE}/acts/{act_id}/act.pdf"
    path = PDFS_DIR / f"{doc_id}.pdf"
    print(f"  → GET {url}")

    resp = session.get(url, timeout=30, stream=True)
    print(f"    Status: {resp.status_code}  Content-Type: {resp.headers.get('Content-Type','?')}")

    if resp.status_code == 200:
        with open(path, "wb") as f:
            for chunk in resp.iter_content(8192):
                f.write(chunk)
        print(f"    ✅ {path}  ({path.stat().st_size/1024:.1f} KB)")
        return str(path)

    print(f"    ❌ {resp.text[:200]}")
    return None


# ─────────────────────────────────────────────
# STEP 2c — File 2: Print page PDF
# Playwright renders /propriete/{doc_id}/print → output/prints/{doc_id}_print.pdf
# ─────────────────────────────────────────────

def download_print_pdf(doc_id: str, page) -> str | None:
    url  = f"https://app.monprospecteur.com/app.html#/propriete/{doc_id}/print"
    path = PRINTS_DIR / f"{doc_id}_print.pdf"
    print(f"  → Rendering {url}")

    page.goto(url, timeout=60000, wait_until="domcontentloaded")
    page.wait_for_load_state("networkidle", timeout=60000)
    page.wait_for_timeout(3000)  # Angular finish rendering

    page.pdf(
        path=str(path),
        format="A4",
        print_background=True,
        margin={"top": "10mm", "bottom": "10mm", "left": "10mm", "right": "10mm"},
    )
    print(f"    ✅ {path}  ({path.stat().st_size/1024:.1f} KB)")
    return str(path)


# ─────────────────────────────────────────────
# FAILED QUEUE — save/load for retry runs
# output/failed/failed_queue.json
# ─────────────────────────────────────────────

FAILED_QUEUE_PATH         = FAILED_DIR / "failed_queue.json"
FAILED_UPLOADS_QUEUE_PATH = FAILED_DIR / "failed_uploads_queue.json"

def save_failed_queue(failed_docs: list):
    """Save raw doc objects that failed so a retry run can pick them up."""
    FAILED_QUEUE_PATH.write_text(
        json.dumps(failed_docs, indent=2, ensure_ascii=False, default=str)
    )
    print(f"\n💾 Failed queue saved → {FAILED_QUEUE_PATH}  ({len(failed_docs)} items)")

def load_failed_queue() -> list:
    if not FAILED_QUEUE_PATH.exists():
        return []
    docs = json.loads(FAILED_QUEUE_PATH.read_text())
    print(f"📂 Loaded {len(docs)} docs from failed queue → {FAILED_QUEUE_PATH}")
    return docs

def clear_failed_queue():
    if FAILED_QUEUE_PATH.exists():
        FAILED_QUEUE_PATH.unlink()
        print(f"🗑  Cleared failed queue")


# ─────────────────────────────────────────────
# FAILED UPLOADS QUEUE
# Separate from scraping failures — these are rows
# that scraped OK but failed to upload to Drive/Sheet
# ─────────────────────────────────────────────

def save_failed_uploads(failed_rows: list):
    """Save rows that failed upload so they can be retried with --retry-uploads."""
    FAILED_UPLOADS_QUEUE_PATH.write_text(
        json.dumps(failed_rows, indent=2, ensure_ascii=False, default=str)
    )
    print(f"\n💾 Failed uploads queue saved → {FAILED_UPLOADS_QUEUE_PATH}  ({len(failed_rows)} items)")

def load_failed_uploads() -> list:
    if not FAILED_UPLOADS_QUEUE_PATH.exists():
        return []
    rows = json.loads(FAILED_UPLOADS_QUEUE_PATH.read_text())
    print(f"📂 Loaded {len(rows)} failed upload rows → {FAILED_UPLOADS_QUEUE_PATH}")
    return rows

def clear_failed_uploads():
    if FAILED_UPLOADS_QUEUE_PATH.exists():
        FAILED_UPLOADS_QUEUE_PATH.unlink()
        print(f"🗑  Cleared failed uploads queue")


# ─────────────────────────────────────────────
# PROCESS ONE DOCUMENT
# ─────────────────────────────────────────────

def process_doc(doc: dict, req_session, page, stats: RunStats, raw_docs_by_id: dict, log=None):
    """
    For one unread document:
      1. Fetch detail API
      2. Download act PDF
      3. Download print PDF
    Records success or failure into stats.
    Returns (detail, act_pdf_path, print_pdf_path) — None on failure.
    """
    doc_id  = doc.get("_id")
    act_id  = doc.get("act")
    address = doc.get("address", "N/A")

    print(f"\n{'─' * 60}")
    print(f"📄 {address}")
    print(f"   _id : {doc_id}")
    print(f"   act : {act_id}")
    print(f"{'─' * 60}")

    detail    = None
    act_pdf   = None
    print_pdf = None
    failed_step = None
    failed_error = None

    # ── Detail API ────────────────────────────────────────────────────
    if log: log.info("Fetching detail API", doc=doc_id)
    try:
        human_delay(1.0, 3.0)
        detail = fetch_document_details(doc_id, req_session) # type: ignore
        if detail is None:
            raise Exception("Empty response from detail API")
        detail_path = DATA_DIR / f"detail_{doc_id}.json"
        detail_path.write_text(json.dumps(detail, indent=2, ensure_ascii=False, default=str))
        if log: log.ok("Detail API fetched", doc=doc_id)
    except Exception as e:
        failed_step  = "detail_api"
        failed_error = e
        print(f"  ❌ Detail API failed: {e}")
        if log: log.error("Detail API failed", error=str(e), doc=doc_id)

    # ── Act PDF ───────────────────────────────────────────────────────
    if not failed_step:
        if log: log.info("Downloading act PDF", doc=doc_id, act=act_id)
        try:
            human_delay(1.5, 4.0)
            act_pdf = download_act_pdf(doc_id, act_id, req_session) # type: ignore
            if act_pdf is None:
                raise Exception("act PDF download returned None")
            if log: log.ok("Act PDF saved", path=act_pdf)
        except Exception as e:
            failed_step  = "act_pdf"
            failed_error = e
            print(f"  ❌ Act PDF failed: {e}")
            if log: log.error("Act PDF failed", error=str(e), doc=doc_id)

    # ── Print PDF ─────────────────────────────────────────────────────
    if not failed_step:
        if log: log.info("Rendering print PDF", doc=doc_id)
        try:
            human_delay(2.0, 4.0)
            print_pdf = download_print_pdf(doc_id, page) # type: ignore
            if print_pdf is None:
                raise Exception("print PDF returned None")
            if log: log.ok("Print PDF saved", path=print_pdf)
        except Exception as e:
            failed_step  = "print_pdf"
            failed_error = e
            print(f"  ❌ Print PDF failed: {e}")
            if log: log.error("Print PDF failed", error=str(e), doc=doc_id)

    # ── Record result ─────────────────────────────────────────────────
    if failed_step:
        stats.record_failure(doc_id, address, failed_step, failed_error)
        raw_docs_by_id[doc_id] = doc
        # Record failed scrape in history
        try:
            from run_history import record_scrape
            record_scrape(
                doc_id      = doc_id, # type: ignore
                address     = address,
                lead_source = doc.get("type", ""),
                run_id      = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S'),
                scrape_ok   = False,
                scrape_error = f"{failed_step}: {failed_error}",
            )
        except Exception:
            pass
        return None, None, None
    else:
        stats.record_success(doc_id, address, act_pdf, print_pdf)
        print(f"\n  ✅ Done: act={act_pdf}  print={print_pdf}")
        return detail, act_pdf, print_pdf


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def scrape(retry_mode: bool = False, test_mode: bool = False):
    """
    retry_mode=False, test_mode=False : normal run — fetch all unread docs
    retry_mode=True                   : retry run  — load failed_queue.json
    test_mode=True                    : take first 3 docs regardless of isRead
    """
    from logger import RunLogger # type: ignore
    stats  = RunStats()
    run_id = stats.run_started_at.strftime('%Y%m%d_%H%M%S')
    log    = RunLogger(run_id)
    failed_raw_docs = {}

    log.step("Run started", mode="test" if test_mode else ("retry" if retry_mode else "normal"))

    with sync_playwright() as p:
        if PROXY_HOST and PROXY_PORT:
            proxy_config = {
                "server":   f"http://{PROXY_HOST}:{PROXY_PORT}",
                "username": PROXY_USER,
                "password": PROXY_PASS,
            }
            log.info("Webshare proxy configured", host=PROXY_HOST, port=PROXY_PORT)
        else:
            proxy_config = None
            log.warn("No proxy configured — using direct connection")

        headless = os.getenv("PLAYWRIGHT_HEADLESS", "true").lower() != "false"
        browser = p.chromium.launch(
            headless=headless,
            args=["--disable-blink-features=AutomationControlled"],
            proxy=proxy_config, # type: ignore
        )
        log.info("Browser launched", headless=headless)

        session_exists = os.path.exists(SESSION_FILE)
        context = browser.new_context(
            storage_state=SESSION_FILE if session_exists else None,
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            viewport={"width": 1440, "height": 900},
            locale="fr-CA",
            timezone_id="America/Toronto",
            extra_http_headers={"Accept-Language": "fr-CA,fr;q=0.9,en-CA;q=0.8,en;q=0.7"},
        )
        page = context.new_page()

        # ── Login ──────────────────────────────────────────────────────
        log.step("Checking session / login")
        if session_exists and is_session_valid(page):
            log.ok("Using saved session")
        else:
            if session_exists:
                os.remove(SESSION_FILE)
            log.info("Starting login flow", url=LOGIN_URL)
            login(page, context)
            log.ok("Login successful — session saved")

        storage_state = json.loads(Path(SESSION_FILE).read_text())
        req_session   = build_requests_session(storage_state)

        # ── Get docs to process ────────────────────────────────────────
        log.step("Fetching documents from API")
        if test_mode:
            log.info("TEST MODE — taking first 3 docs (read or unread)")
            all_docs        = fetch_all_documents(req_session, log=log)
            docs_to_process = all_docs[:3]
            stats.total_fetched = len(all_docs)
            stats.total_unread  = len(docs_to_process)
            raw_path = DATA_DIR / "raw_documents.json"
            raw_path.write_text(json.dumps(all_docs, indent=2, ensure_ascii=False))

        elif retry_mode:
            log.info("RETRY MODE — loading failed queue")
            docs_to_process = load_failed_queue()
            if not docs_to_process:
                log.ok("No failed docs to retry — exiting")
                browser.close()
                return
            stats.total_fetched = len(docs_to_process)
            stats.total_unread  = len(docs_to_process)
            all_docs = docs_to_process

        else:
            all_docs        = fetch_all_documents(req_session, log=log)
            docs_to_process = filter_unread(all_docs, stats)
            raw_path = DATA_DIR / "raw_documents.json"
            raw_path.write_text(json.dumps(all_docs, indent=2, ensure_ascii=False))

        log.ok(f"Documents ready", total=len(all_docs), to_process=len(docs_to_process))

        if not docs_to_process:
            log.ok("No unread leads to process — exiting")
            stats.run_finished_at = datetime.now(timezone.utc)
            stats.print_summary()
            log.finish(0, 0)
            browser.close()
            return

        log.step(f"Processing {len(docs_to_process)} leads")

        # ── Process each doc ───────────────────────────────────────────
        source_motivation_map = {}  # doc_id -> source motivation string

        for i, doc in enumerate(docs_to_process):
            doc_id  = doc.get("_id", "?")
            address = doc.get("address", "N/A")
            log.step(f"Processing lead [{i+1}/{len(docs_to_process)}]", address=address, id=doc_id)

            process_doc(doc, req_session, page, stats, failed_raw_docs, log=log)

            # ── Property history API ───────────────────────────────────
            if doc_id and doc_id != "?":
                human_delay(1.0, 2.0)
                motivation = fetch_property_history(doc_id, req_session, log=log)
                source_motivation_map[doc_id] = motivation

            # ── Webhook call ───────────────────────────────────────────
            if WEBHOOK_URL:
                try:
                    log.info("Calling webhook", url=WEBHOOK_URL)
                    wh_resp = req_session.get(WEBHOOK_URL, timeout=15)
                    log.ok(f"Webhook response", status=wh_resp.status_code)
                except Exception as e:
                    log.error("Webhook call failed", error=str(e))

            log.ok(f"Lead completed", address=address)

            if i < len(docs_to_process) - 1:
                between_docs_delay(log=log)

        # ── Save failed queue for retry ────────────────────────────────
        if failed_raw_docs:
            save_failed_queue(list(failed_raw_docs.values()))
        else:
            # All succeeded — clear any previous failed queue
            clear_failed_queue()

        # ── Save full run stats ────────────────────────────────────────
        stats.run_finished_at = datetime.now(timezone.utc)
        stats_path = DATA_DIR / f"run_stats_{stats.run_started_at.strftime('%Y%m%d_%H%M%S')}.json"
        stats_path.write_text(json.dumps(stats.summary, indent=2, ensure_ascii=False))
        print(f"\n💾 Run stats → {stats_path}")

        stats.print_summary()

        # ── Upload to Drive + Excel + Sheet + Email ──────────────────
        from excel_uploader  import clean_lead, write_leads_to_excel
        from drive_uploader  import upload_lead_files, GoogleAuthError as DriveAuthError
        from sheets_uploader import ensure_header_row, append_rows_to_sheet, GoogleAuthError as SheetAuthError
        from email_sender    import send_summary_email
        from run_history     import record_scrape, record_drive_result, record_sheet_result, record_excel_result

        if test_mode:
            print("\n🧪 TEST MODE — running full pipeline on 3 docs")

        run_id = stats.run_started_at.strftime('%Y%m%d_%H%M%S')
        UPLOAD_MAX_RETRIES = 5
        UPLOAD_RETRY_WAIT  = 5  # seconds between upload retries

        excel_rows  = []
        excel_path  = str(DATA_DIR / f"leads_{stats.run_started_at.strftime('%Y%m%d_%H%M%S')}.xlsx")
        source_docs = all_docs if not retry_mode else docs_to_process

        for result in stats.succeeded:
            doc_id    = result["id"]
            address   = result["address"]
            act_pdf   = result.get("act_pdf")
            print_pdf = result.get("print_pdf")

            detail_path = DATA_DIR / f"detail_{doc_id}.json"
            detail_doc  = json.loads(detail_path.read_text()) if detail_path.exists() else None
            list_doc    = next((d for d in source_docs if d.get("_id") == doc_id), {})
            row         = clean_lead(list_doc, detail_doc) # type: ignore

            # Fill Source motivation from property_history API result
            row["Source motivation"] = source_motivation_map.get(doc_id, "")
            log.info("Source motivation set", value=row["Source motivation"], doc=doc_id)

            # Record scrape in history (drive/sheet pending at this point)
            record_scrape(
                doc_id           = doc_id,
                address          = address,
                lead_source      = row.get("Lead Source", ""),
                run_id           = run_id,
                act_pdf          = act_pdf,
                print_pdf        = print_pdf,
                scrape_ok        = True,
                reference_number = row.get("Reference Number", ""),
            )

            # ── Drive upload with in-run retries ──────────────────────
            drive_url    = ""
            drive_log    = []
            drive_ok     = False
            drive_err    = None

            for attempt in range(1, UPLOAD_MAX_RETRIES + 1):
                try:
                    drive_url = upload_lead_files(
                        doc_id     = doc_id,
                        lead_type  = row.get("Lead Source", ""),
                        street_num = row.get("Other Street Number", ""),
                        street     = row.get("Other Street", ""),
                        act_pdf    = act_pdf,
                        print_pdf  = print_pdf,
                    )
                    drive_ok  = True
                    drive_err = None
                    if attempt > 1:
                        msg = f"✅ Drive upload succeeded on attempt {attempt}"
                        print(f"  {msg}")
                        drive_log.append(msg)
                    break
                except DriveAuthError as e:
                    # Auth errors won't fix themselves — stop immediately
                    drive_err = str(e)
                    drive_log.append(f"❌ Auth error: {e}")
                    print(f"  ❌ Drive auth error — stopping retries: {e}")
                    break
                except Exception as e:
                    drive_err = str(e)
                    err_msg   = f"attempt {attempt}/{UPLOAD_MAX_RETRIES}: {e}"
                    print(f"  ❌ Drive upload failed — {err_msg}")
                    drive_log.append(f"❌ Drive {err_msg}")
                    if attempt < UPLOAD_MAX_RETRIES:
                        print(f"  ⏳ Retrying in {UPLOAD_RETRY_WAIT}s...")
                        time.sleep(UPLOAD_RETRY_WAIT)

            # Save drive result to history
            record_drive_result(
                doc_id   = doc_id,
                ok       = drive_ok,
                url      = drive_url,
                error    = drive_err, # type: ignore
                attempts = len(drive_log) + 1,
            )

            row["Google Drive"] = drive_url
            result["drive_url"]        = drive_url
            result["drive_upload_ok"]  = drive_ok
            result["drive_upload_log"] = drive_log

            excel_rows.append(row)

        # ── Write Excel ────────────────────────────────────────────────
        excel_ok = False
        if excel_rows:
            try:
                write_leads_to_excel(excel_rows, excel_path)
                excel_ok = True
                record_excel_result([r.get("Reference Number","") and next(
                    (s["id"] for s in stats.succeeded if s.get("drive_url","") == r.get("Google Drive","")), ""
                ) for r in excel_rows], ok=True, path=excel_path)
            except Exception as e:
                print(f"  ❌ Excel write failed: {e}")
                record_excel_result(
                    [s["id"] for s in stats.succeeded], ok=False, error=str(e)
                )

        # ── Append to Google Sheet with in-run retries ─────────────────
        sheet_ok  = False
        sheet_log = []
        sheet_err = None

        if excel_rows:
            try:
                ensure_header_row()
            except SheetAuthError as e:
                sheet_err = str(e)
                sheet_log.append(f"❌ Auth error: {e}")
                print(f"  ❌ Sheet auth error: {e}")

            if not sheet_err:
                for attempt in range(1, UPLOAD_MAX_RETRIES + 1):
                    try:
                        append_rows_to_sheet(excel_rows)
                        sheet_ok  = True
                        sheet_err = None
                        if attempt > 1:
                            msg = f"✅ Sheet upload succeeded on attempt {attempt}"
                            print(f"  {msg}")
                            sheet_log.append(msg)
                        break
                    except SheetAuthError as e:
                        sheet_err = str(e)
                        sheet_log.append(f"❌ Auth error: {e}")
                        print(f"  ❌ Sheet auth error — stopping retries: {e}")
                        break
                    except Exception as e:
                        sheet_err = str(e)
                        err_msg   = f"attempt {attempt}/{UPLOAD_MAX_RETRIES}: {e}"
                        print(f"  ❌ Sheet upload failed — {err_msg}")
                        sheet_log.append(f"❌ Sheet {err_msg}")
                        if attempt < UPLOAD_MAX_RETRIES:
                            print(f"  ⏳ Retrying in {UPLOAD_RETRY_WAIT}s...")
                            time.sleep(UPLOAD_RETRY_WAIT)

        # Save sheet result to history
        record_sheet_result(
            doc_ids = [s["id"] for s in stats.succeeded],
            ok      = sheet_ok,
            error   = sheet_err, # type: ignore
        )

        # Record sheet result in stats
        stats.upload_sheet_ok  = sheet_ok # type: ignore
        stats.upload_sheet_log = sheet_log # type: ignore

        # ── Save failed uploads queue for manual --retry-uploads ───────
        permanently_failed = [
            r for r in stats.succeeded
            if not r.get("drive_upload_ok", True)
        ]
        sheet_failed_rows = excel_rows if not sheet_ok else []

        all_failed_uploads = []
        for r in permanently_failed:
            row_data = next((x for x in excel_rows if x.get("Reference Number") and r["id"] in json.dumps(x)), {})
            all_failed_uploads.append({
                "_retry_type": "drive",
                "_doc_id":     r["id"],
                "_act_pdf":    r.get("act_pdf"),
                "_print_pdf":  r.get("print_pdf"),
                "_error":      " | ".join(r.get("drive_upload_log", [])),
                **row_data,
            })
        for row in sheet_failed_rows:
            all_failed_uploads.append({
                "_retry_type": "sheet",
                "_error":      " | ".join(sheet_log),
                **row,
            })

        if all_failed_uploads:
            save_failed_uploads(all_failed_uploads)
            print(f"\n⚠️  {len(all_failed_uploads)} upload(s) still failing after {UPLOAD_MAX_RETRIES} attempts")
            print(f"   Run: python main.py --retry-uploads")
        else:
            clear_failed_uploads()

        # ── Send summary email ─────────────────────────────────────────
        # Enrich stats.summary with upload details before sending
        for r in stats.succeeded:
            r["drive_upload_ok"]  = r.get("drive_upload_ok", True)
            r["drive_upload_log"] = r.get("drive_upload_log", [])
        stats.upload_sheet_ok  = getattr(stats, "upload_sheet_ok", True) # type: ignore
        stats.upload_sheet_log = getattr(stats, "upload_sheet_log", []) # type: ignore

        # Finish log and attach to email
        log.finish(
            succeeded = len(stats.succeeded),
            failed    = len(stats.failed),
        )

        send_summary_email(
            stats_summary     = stats.summary,
            sheet_ok          = sheet_ok,
            sheet_log         = sheet_log,
            excel_path        = excel_path if excel_rows else None, # type: ignore
            extra_attachments = [log.path],
        )

        browser.close()


def retry_uploads():
    """
    Retry any rows that failed Drive or Sheet upload in a previous run.
    Loads from output/failed/failed_uploads_queue.json
    Run with: python main.py --retry-uploads
    """
    from excel_uploader  import write_leads_to_excel
    from drive_uploader  import upload_lead_files
    from sheets_uploader import ensure_header_row, append_rows_to_sheet

    rows = load_failed_uploads()
    if not rows:
        print("✅ No failed uploads to retry")
        return

    print(f"\n🔁 Retrying {len(rows)} failed upload(s)...")

    still_failed  = []
    sheet_rows    = []
    excel_rows    = []

    for row in rows:
        retry_type = row.get("_retry_type", "drive")
        doc_id     = row.get("_doc_id", "")
        act_pdf    = row.get("_act_pdf", "")
        print_pdf  = row.get("_print_pdf", "")

        # Strip internal retry keys before uploading
        clean_row = {k: v for k, v in row.items() if not k.startswith("_")}

        if retry_type == "drive":
            try:
                drive_url = upload_lead_files(
                    doc_id     = doc_id,
                    lead_type  = clean_row.get("Lead Source", ""),
                    street_num = clean_row.get("Other Street Number", ""),
                    street     = clean_row.get("Other Street", ""),
                    act_pdf    = act_pdf,
                    print_pdf  = print_pdf,
                )
                clean_row["Google Drive"] = drive_url
                sheet_rows.append(clean_row)
                excel_rows.append(clean_row)
                print(f"  ✅ Drive retry succeeded: {doc_id}")
            except Exception as e:
                print(f"  ❌ Drive retry failed again: {doc_id} — {e}")
                still_failed.append({**row, "_error": str(e)})

        elif retry_type == "sheet":
            sheet_rows.append(clean_row)
            excel_rows.append(clean_row)

    # Retry sheet upload
    if sheet_rows:
        try:
            ensure_header_row()
            append_rows_to_sheet(sheet_rows)
            print(f"  ✅ Sheet retry succeeded: {len(sheet_rows)} rows")
        except Exception as e:
            print(f"  ❌ Sheet retry failed: {e}")
            for row in sheet_rows:
                still_failed.append({"_retry_type": "sheet", "_error": str(e), **row})

    # Write Excel for successfully retried rows
    if excel_rows:
        retry_excel_path = str(DATA_DIR / f"leads_retry_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.xlsx")
        write_leads_to_excel(excel_rows, retry_excel_path)

    # Update failed uploads queue
    if still_failed:
        save_failed_uploads(still_failed)
        print(f"\n⚠️  {len(still_failed)} item(s) still failing — check output/failed/failed_uploads_queue.json")
    else:
        clear_failed_uploads()
        print("\n✅ All failed uploads retried successfully")


if __name__ == "__main__":
    import sys
    retry         = "--retry"         in sys.argv
    test          = "--test"          in sys.argv
    retry_uploads_flag = "--retry-uploads" in sys.argv

    if sum([retry, test, retry_uploads_flag]) > 1:
        print("❌ Cannot combine --retry, --test, and --retry-uploads")
        sys.exit(1)

    if retry_uploads_flag:
        retry_uploads()
    else:
        scrape(retry_mode=retry, test_mode=test)