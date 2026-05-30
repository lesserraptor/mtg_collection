#!/usr/bin/env python3
"""Backfill draft results from MTGA Player.log files.

Scans Player.log and Player-prev.log for draft start events (EventJoin)
and prize claim responses (EventClaimPrize response JSON), pairs them
by EventName, and inserts missing rows into draft_results.

Usage:
    python scripts/backfill_draft_results.py
"""

import json
import sys
import logging
from datetime import date, datetime
from pathlib import Path

# Ensure project root is on path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.db.schema import open_db, create_schema, create_indexes

logger = logging.getLogger(__name__)

# Alchemy prefixes to strip (mirrors log_scanner.py)
_ALCHEMY_PREFIXES = ("Y25", "Y", "A")

# Default entry costs per format (mirrors log_scanner.py)
_DEFAULT_COST = {
    "QuickDraft": 5000,
    "PremierDraft": 10000,
    "PickTwoDraft": 6000,
    "TradDraft": 10000,
}


def strip_alchemy_prefix(code: str) -> str:
    """Remove known Alchemy prefixes from set code."""
    for prefix in _ALCHEMY_PREFIXES:
        if code.startswith(prefix) and len(code) > len(prefix):
            stripped = code[len(prefix):]
            if stripped.isalpha() and 2 <= len(stripped) <= 4:
                return stripped.upper()
    return code.upper()


def extract_set_format(event_name: str) -> tuple[str, str]:
    """Extract set_code and format from event name like 'PickTwoDraft_SOS_20260421'."""
    parts = event_name.split("_")
    draft_type = parts[0] if parts else ""
    set_code_raw = ""
    for i, part in enumerate(parts):
        if part.isalpha() and 2 <= len(part) <= 5 and i > 0:
            set_code_raw = part
            break
    if not set_code_raw and len(parts) >= 3:
        set_code_raw = parts[2]
    set_code = strip_alchemy_prefix(set_code_raw) if set_code_raw else ""

    if draft_type == "Trad" and len(parts) >= 2 and parts[1] == "Draft":
        fmt = "TradDraft"
    elif "PickTwoDraft" in parts:
        fmt = "PickTwoDraft"
    elif draft_type == "BotDraft" or "QuickDraft" in event_name:
        fmt = "QuickDraft"
    else:
        fmt = "PremierDraft"

    return set_code, fmt


def parse_event_join(line: str) -> dict | None:
    """Parse EventJoin line -> {event_name, cost_gold} or None."""
    # Find JSON after the prefix
    prefix = "[UnityCrossThreadLogger]==> EventJoin "
    idx = line.find(prefix)
    if idx == -1:
        return None
    try:
        outer = json.loads(line[idx + len(prefix):])
        request_str = outer.get("request", "{}")
        request = json.loads(request_str) if isinstance(request_str, str) else request_str
        event_name = request.get("EventName", "")
        if not event_name:
            return None
        cost = request.get("EntryCurrencyPaid", 0) or 0
        if not isinstance(cost, (int, float)):
            cost = 0
        return {"event_name": event_name, "cost_gold": int(cost)}
    except (json.JSONDecodeError, KeyError, TypeError, ValueError):
        return None


def parse_prize_response(line: str) -> dict | None:
    """Parse EventClaimPrize response JSON -> {event_name, wins, losses, gems} or None."""
    if '"InternalEventName"' not in line or '"InventoryInfo"' not in line:
        return None
    try:
        data = json.loads(line)
        course = data.get("Course", {})
        event_name = course.get("InternalEventName", "")
        if not event_name or "Draft" not in event_name:
            return None
        # Only consider completed drafts (has wins/gems data)
        module = course.get("CurrentModule", "")
        if module not in ("Complete", "MatchResults"):
            return None
        wins = course.get("CurrentWins", 0) or 0
        losses = course.get("CurrentLosses", 0) or 0
        inv = data.get("InventoryInfo", {})
        gems = 0
        changes = inv.get("Changes", [])
        if changes:
            gems = changes[0].get("InventoryGems", 0)
        return {"event_name": event_name, "wins": int(wins), "losses": int(losses), "gems": int(gems)}
    except (json.JSONDecodeError, KeyError, TypeError, ValueError):
        return None


def scan_log(path: Path) -> tuple[dict[str, int], dict[str, dict]]:
    """Scan a single log file. Returns (costs_by_event, prizes_by_event)."""
    costs: dict[str, int] = {}
    prizes: dict[str, dict] = {}

    if not path.exists():
        print(f"  Skipping (not found): {path}")
        return costs, prizes

    size = path.stat().st_size
    print(f"  Scanning {path.name} ({size / 1024 / 1024:.1f} MB)")

    with open(path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            # Check for EventJoin (draft start with cost)
            result = parse_event_join(line)
            if result and result["event_name"] not in costs:
                costs[result["event_name"]] = result["cost_gold"]

            # Check for EventClaimPrize response JSON
            result2 = parse_prize_response(line)
            if result2 and result2["event_name"] not in prizes:
                prizes[result2["event_name"]] = {
                    "wins": result2["wins"],
                    "losses": result2["losses"],
                    "gems": result2["gems"],
                }

    return costs, prizes


def main():
    print("=== Draft Results Backfill ===\n")

    # Find log files
    from src.db.log_parser import LOG_PATHS, find_player_log
    log_path = find_player_log()
    if not log_path or not log_path.exists():
        print("Error: Player.log not found")
        sys.exit(1)

    prev_log = log_path.with_name(log_path.name.replace(".log", "-prev.log"))

    all_costs: dict[str, int] = {}
    all_prizes: dict[str, dict] = {}

    for lp in [log_path, prev_log]:
        costs, prizes = scan_log(lp)
        all_costs.update(costs)
        all_prizes.update(prizes)

    print(f"\nFound {len(all_costs)} draft start events")
    print(f"Found {len(all_prizes)} prize claim responses")

    # Merge by event name
    event_names = set(all_costs.keys()) | set(all_prizes.keys())
    matched = 0
    cost_only = 0
    prize_only = 0

    db_path = Path(__file__).parent.parent / "data" / "mtga_collection.db"
    conn = open_db(db_path)
    create_schema(conn)
    create_indexes(conn)

    # Get existing event names to avoid re-inserting
    existing = set()
    for row in conn.execute("SELECT DISTINCT date || '|' || set_code || '|' || format || '|' || wins || '|' || losses FROM draft_results").fetchall():
        existing.add(row[0])

    inserted = 0
    skipped_existing = 0
    skipped_no_cost = 0
    skipped_no_prize = 0
    today = date.today().isoformat()

    for event_name in sorted(event_names):
        cost = all_costs.get(event_name, 0)
        prize = all_prizes.get(event_name, None)

        set_code, fmt = extract_set_format(event_name)
        if not set_code:
            continue

        if prize is None:
            prize_only += 1
            continue

        wins = prize["wins"]
        losses = prize["losses"]
        gems = prize["gems"]

        # Use cost from EventJoin, or format default
        cost_gold = cost if cost > 0 else _DEFAULT_COST.get(fmt, 0)
        if cost_gold == 0:
            skipped_no_cost += 1
            continue

        # Use today's date (event name suffix is event release date, not play date)
        event_date = today

        trophy = 1 if (fmt == "PickTwoDraft" and wins >= 4) or (fmt != "PickTwoDraft" and wins >= 7) else 0

        # Check if already exists (rough dedup by event signature)
        dedup_key = f"{event_date}|{set_code}|{fmt}|{wins}|{losses}"
        if dedup_key in existing:
            skipped_existing += 1
            continue

        conn.execute(
            """INSERT OR IGNORE INTO draft_results (date, set_code, format, wins, losses, cost_gold, winnings_gems, trophy)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (event_date, set_code, fmt, wins, losses, cost_gold, gems, trophy),
        )
        if conn.total_changes > 0:
            inserted += 1
            print(f"  + {event_date} {set_code} {fmt} {wins}-{losses} cost={cost_gold} gems={gems}")
        matched += 1

    conn.commit()

    count = conn.execute("SELECT COUNT(*) as cnt FROM draft_results").fetchone()[0]

    print(f"\n=== Summary ===")
    print(f"  Matched events:      {matched}")
    print(f"  Inserted:            {inserted}")
    print(f"  Skipped (exists):    {skipped_existing}")
    print(f"  Skipped (no cost):   {skipped_no_cost}")
    print(f"  Skipped (no prize):  {skipped_no_prize}")
    print(f"  Prize-only (no cost): {prize_only}")
    print(f"  Total in DB:         {count}")


if __name__ == "__main__":
    main()
