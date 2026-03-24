"""
scheduler.py
────────────
Runs inside the scheduler container.
Triggers the scraper daily at 05:00 Toronto time.
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
PROJECT_DIR = os.getenv("PROJECT_DIR", "/app")


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
    log(f"Scheduled trigger — project dir: {PROJECT_DIR}")
    log("Starting scraper container...")
    try:
        result = subprocess.run(
            ["docker-compose", "run", "--rm", "scraper"],
            cwd=PROJECT_DIR,
            capture_output=True,
            text=True,
            timeout=36000,
        )
        # Log both stdout and stderr so we can see exactly what happened
        if result.stdout:
            for line in result.stdout.strip().split("\n"):
                log(f"[stdout] {line}")
        if result.stderr:
            for line in result.stderr.strip().split("\n"):
                log(f"[stderr] {line}")

        if result.returncode == 0:
            log("Scraper finished successfully")
        else:
            log(f"Scraper finished with exit code {result.returncode}")
    except subprocess.TimeoutExpired:
        log("ERROR: Scraper timed out after 10 hours")
    except Exception as e:
        log(f"ERROR: Could not start scraper: {e}")
    log("=" * 50)


def main():
    log(f"Scheduler started — daily run at {RUN_HOUR:02d}:{RUN_MINUTE:02d} Toronto time")
    log(f"Project directory: {PROJECT_DIR}")
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