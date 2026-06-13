"""results_importer.py — Standalone draft results importer from Player.log.

Completely independent of the draft scanner (DraftState, file_offset, _process_line).
Uses its own offset tracking (via meta table key "draft_results_offset")
so it can incrementally scan for new completed draft events.

Designed to be called from:
  - Wallet watcher (on file change)
  - App startup (for past results)
  - Manual trigger from UI
"""

import json
import logging
import sqlite3
from datetime import date
from pathlib import Path

logger = logging.getLogger(__name__)


def scan_draft_results_from_log(log_path: Path, db: sqlite3.Connection) -> int:
    """Scan Player.log for completed draft events and insert into draft_results.

    Opens the log file, seeks to the last known offset (stored in meta table
    as 'draft_results_offset'), reads new lines, and scans for prize response
    lines containing InternalEventName + InventoryInfo with CurrentModule=Complete
    and 'Draft' in the event name.

    Uses its OWN offset (not the wallet watcher's or scanner's).

    Args:
        log_path: Path to Player.log
        db: SQLite database connection

    Returns:
        Number of new draft results inserted
    """
    try:
        # Lazy import — log_scanner pulls in watchdog at module level
        from src.draft.log_scanner import _DEFAULT_COST, _strip_alchemy_prefix

        # 1. Read offset from meta table
        row = db.execute(
            "SELECT value FROM meta WHERE key = 'draft_results_offset'"
        ).fetchone()
        offset = int(row[0]) if row else 0

        # 2. Check file size; reset on log rotation
        file_size = log_path.stat().st_size
        if offset > file_size:
            logger.info(
                "draft_results: offset %d > file size %d, resetting to 0 (log rotation)",
                offset, file_size,
            )
            offset = 0

        # 3. Open and seek to offset, read new lines
        with open(log_path, "r", encoding="utf-8", errors="replace") as f:
            f.seek(offset)
            lines = f.readlines()
            new_offset = f.tell()

        inserted = 0
        today = date.today().isoformat()

        # 4. Process each line — reuse prize response parsing logic from log_scanner
        for line in lines:
            try:
                if '"InternalEventName"' not in line or '"InventoryInfo"' not in line:
                    continue

                data = json.loads(line)
                course = data.get("Course", {})
                event_name = course.get("InternalEventName", "")
                if not event_name or "Draft" not in event_name:
                    continue

                # Only process completed drafts (has wins/gems data)
                module = course.get("CurrentModule", "")
                if module not in ("Complete", "MatchResults"):
                    continue

                # Parse event name for set_code and format
                parts = event_name.split("_")
                draft_type = parts[0] if parts else ""
                set_code_raw = ""
                for i, part in enumerate(parts):
                    if part.isalpha() and 2 <= len(part) <= 5 and i > 0:
                        set_code_raw = part
                        break
                if not set_code_raw and len(parts) >= 3:
                    set_code_raw = parts[2]
                set_code = _strip_alchemy_prefix(set_code_raw) if set_code_raw else ""
                if not set_code:
                    continue

                if draft_type == "Trad" and len(parts) >= 2 and parts[1] == "Draft":
                    fmt = "TradDraft"
                elif "PickTwoDraft" in parts:
                    fmt = "PickTwoDraft"
                elif draft_type == "BotDraft" or "QuickDraft" in event_name:
                    fmt = "QuickDraft"
                else:
                    fmt = "PremierDraft"

                wins = course.get("CurrentWins", 0) or 0
                losses = course.get("CurrentLosses", 0) or 0

                # Gems from InventoryInfo.Changes[0].InventoryGems
                inv = data.get("InventoryInfo", {})
                gems = 0
                changes = inv.get("Changes", [])
                if changes:
                    gems = changes[0].get("InventoryGems", 0)

                # Entry cost from format default (no DraftState available)
                cost = _DEFAULT_COST.get(fmt, 0)
                if cost == 0:
                    continue

                # Dedup check: same date + set_code + format + wins + losses
                existing = db.execute(
                    """SELECT id FROM draft_results
                       WHERE date=? AND set_code=? AND format=? AND wins=? AND losses=?""",
                    (today, set_code, fmt, wins, losses),
                ).fetchone()
                if existing:
                    continue

                trophy = 1 if (fmt == "PickTwoDraft" and wins >= 4) or (fmt != "PickTwoDraft" and wins >= 7) else 0
                db.execute(
                    """INSERT INTO draft_results (date, set_code, format, wins, losses, cost_gold, winnings_gems, trophy)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                    (today, set_code, fmt, wins, losses, cost, gems, trophy),
                )
                db.commit()
                inserted += 1
                logger.info(
                    "draft_results: imported — set=%s format=%s wins=%d losses=%d gems=%d",
                    set_code, fmt, wins, losses, gems,
                )

            except (json.JSONDecodeError, KeyError, TypeError, ValueError):
                logger.debug("draft_results: failed to parse line", exc_info=False)
                continue

        # 5. Update offset in meta table
        db.execute(
            "INSERT OR REPLACE INTO meta (key, value) VALUES ('draft_results_offset', ?)",
            (str(new_offset),),
        )
        db.commit()

        return inserted

    except Exception:
        logger.exception("draft_results: error during scan")
        return 0
