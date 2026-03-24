"""
scheduler.py
────────────
Runs inside the scheduler container.
Triggers the scraper daily at 05:00 Toronto time by running:
    docker compose run --rm scraper

Logs every trigger to output/logs/scheduler.log
"""

import time
import subprocess
import os
from datetime import datetime
import zoneinfo

TORONTO_TZ  = zoneinfo.ZoneInfo("America/Toronto")
RUN_HOUR    = int(os.getenv("SCHEDULE_HOUR",   "5"))
RUN_MINUTE  = int(os.getenv("SCHEDULE_MINUTE", "0"))
LOG_FILE    = "/app/output/logs/scheduler.log"


def log(msg: str):
    now  = datetime.now(TORONTO_TZ).strftime("%Y-%m-%d %H:%M:%S %Z")
    line = f"[{now}] {msg}"
    print(line, flush=True)
    try:
        os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
        with open(LOG_FILE, "a") as f:
            f.write(line + "\n")
    except Exception:
        pass


def run_scraper():
    log("=" * 50)
    log("Scheduled trigger — starting scraper container...")
    try:
        result = subprocess.run(
            ["docker", "compose", "run", "--rm", "scraper"],
            cwd=os.getenv("PROJECT_DIR", "/app"),
            timeout=36000,  # 10 hour max (200 leads × ~40s delay + processing time)
        )
        if result.returncode == 0:
            log("Scraper finished successfully")
        else:
            log(f"Scraper finished with exit code {result.returncode}")
    except subprocess.TimeoutExpired:
        log("ERROR: Scraper timed out after 2 hours")
    except Exception as e:
        log(f"ERROR: Could not start scraper: {e}")
    log("=" * 50)


def main():
    log(f"Scheduler started — daily run at {RUN_HOUR:02d}:{RUN_MINUTE:02d} Toronto time")
    last_run_date = None

    while True:
        now         = datetime.now(TORONTO_TZ)
        today       = now.date()
        is_run_time = now.hour == RUN_HOUR and now.minute == RUN_MINUTE

        if is_run_time and last_run_date != today:
            last_run_date = today
            run_scraper()

        time.sleep(30)


if __name__ == "__main__":
    main()