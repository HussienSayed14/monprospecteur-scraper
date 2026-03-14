"""
logger.py
─────────
Structured logger for each scraper run.
Writes timestamped entries to output/logs/run_YYYYMMDD_HHMMSS.log

Usage:
    from logger import RunLogger
    log = RunLogger()

    log.info("VPN connected", ip="82.23.96.252")
    log.info("Browser opened", url="monprospecteur.com")
    log.ok("Login successful")
    log.error("Drive upload failed", error="Invalid Value", doc="123 Rue Test")
"""

import json
from pathlib import Path
from datetime import datetime, timezone
import zoneinfo

TORONTO_TZ = zoneinfo.ZoneInfo("America/Toronto")
LOGS_DIR   = Path("output/logs")
LOGS_DIR.mkdir(parents=True, exist_ok=True)


class RunLogger:
    def __init__(self, run_id: str = None): # type: ignore
        self.run_id   = run_id or datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        self.log_path = LOGS_DIR / f"run_{self.run_id}.log"
        self._entries = []
        self._write_header()

    def _now(self) -> str:
        return datetime.now(TORONTO_TZ).strftime("%H:%M:%S %Z")

    def _write_header(self):
        self._append_raw(f"{'='*60}")
        self._append_raw(f"MonProspecteur Scraper — Run {self.run_id}")
        self._append_raw(f"Started: {datetime.now(TORONTO_TZ).strftime('%Y-%m-%d %H:%M:%S %Z')}")
        self._append_raw(f"{'='*60}")

    def _append_raw(self, line: str):
        with open(self.log_path, "a", encoding="utf-8") as f:
            f.write(line + "\n")

    def _log(self, level: str, message: str, **kwargs):
        parts = [f"[{self._now()}] [{level}] {message}"]
        if kwargs:
            parts.append("  " + "  ".join(f"{k}={v}" for k, v in kwargs.items()))
        line = "\n".join(parts) if kwargs else parts[0]
        print(line)
        self._append_raw(line)
        self._entries.append({
            "time":    self._now(),
            "level":   level,
            "message": message,
            **kwargs,
        })

    def info(self, message: str, **kwargs):
        self._log("INFO ", message, **kwargs)

    def ok(self, message: str, **kwargs):
        self._log("OK   ", message, **kwargs)

    def warn(self, message: str, **kwargs):
        self._log("WARN ", message, **kwargs)

    def error(self, message: str, **kwargs):
        self._log("ERROR", message, **kwargs)

    def step(self, message: str, **kwargs):
        """For major pipeline steps — adds a blank line before for readability."""
        self._append_raw("")
        self._log("STEP ", message, **kwargs)

    def finish(self, succeeded: int, failed: int):
        self._append_raw("")
        self._append_raw(f"{'='*60}")
        self._append_raw(f"Run finished: {datetime.now(TORONTO_TZ).strftime('%Y-%m-%d %H:%M:%S %Z')}")
        self._append_raw(f"Succeeded: {succeeded}  |  Failed: {failed}")
        self._append_raw(f"{'='*60}")
        print(f"\n📄 Log saved → {self.log_path.resolve()}")

    @property
    def path(self) -> str:
        return str(self.log_path.resolve())