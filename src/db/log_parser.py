"""Player.log parser for MTGA Arena deck extraction.

Provides path discovery and deck parsing from the MTGA game log.
Two event types are supported:
  - StartHook: Comprehensive snapshot of all current decks, emitted at login.
  - DeckUpsertDeckV2: Individual deck create/update events, used as fallback.

Exported functions:
  find_player_log() -> Path | None
  parse_log_decks(log_path: Path) -> list[dict]
"""

import json
import os
from pathlib import Path

LOG_PATHS = [
    # Configurable via env var (for dev: point at project data/ dir)
    os.environ.get("MTGA_LOG_PATH", ""),
    # Linux / Steam (Proton) — production path when app runs on the Linux machine
    str(Path.home() / ".local/share/Steam/steamapps/compatdata/2141910/pfx/drive_c/users/steamuser/AppData/LocalLow/Wizards Of The Coast/MTGA/Player.log"),
    # macOS
    str(Path.home() / "Library/Logs/Wizards Of The Coast/MTGA/Player.log"),
]


def find_player_log(db=None) -> Path | None:
    """Return the first Player.log path that exists, or None.

    When db is provided, checks the meta table for a saved default path first.
    Falls back to LOG_PATHS in order: env var override first, then platform standard paths.
    Skips empty strings.

    Args:
        db: Optional sqlite3.Connection. If provided, checks meta table for
            'default_log_path' before falling back to LOG_PATHS.
    """
    if db is not None:
        row = db.execute("SELECT value FROM meta WHERE key = 'default_log_path'").fetchone()
        if row and row["value"]:
            p = Path(row["value"])
            if p.exists():
                return p
    for p in LOG_PATHS:
        if not p:
            continue
        candidate = Path(p)
        if candidate.exists():
            return candidate
    return None


def _parse_starthook(data: dict) -> list[dict]:
    """Extract deck list from a parsed StartHook JSON payload."""
    decks_by_id = data.get("Decks", {})
    summaries = data.get("DeckSummariesV2", [])

    # Build name and format lookup from summaries
    name_lookup: dict[str, str] = {}
    format_lookup: dict[str, str] = {}
    for summary in summaries:
        deck_id = summary.get("DeckId", "")
        name_lookup[deck_id] = summary.get("Name", "")
        attrs = summary.get("Attributes", [])
        fmt = next((a["value"] for a in attrs if a.get("name") == "Format"), "")
        format_lookup[deck_id] = fmt

    result = []
    for deck_id, deck in decks_by_id.items():
        name = name_lookup.get(deck_id, deck_id)
        if name.startswith("?=?Loc/Decks/Precon/"):
            continue

        mainboard_raw = deck.get("MainDeck", [])
        if not mainboard_raw:
            continue

        mainboard = [
            {"arena_id": int(entry["cardId"]), "quantity": int(entry["quantity"])}
            for entry in mainboard_raw
        ]

        sideboard_raw = deck.get("Sideboard", [])
        sideboard = [
            {"arena_id": int(entry["cardId"]), "quantity": int(entry["quantity"])}
            for entry in sideboard_raw
        ]

        commander_raw = deck.get("CommandZone", [])
        commander = [
            {"arena_id": int(entry["cardId"]), "quantity": int(entry["quantity"])}
            for entry in commander_raw
        ]

        result.append({
            "deck_id": deck_id,
            "name": name,
            "format": format_lookup.get(deck_id, ""),
            "mainboard": mainboard,
            "sideboard": sideboard,
            "commander": commander,
        })

    return result


def _parse_upsert_events(lines: list[str]) -> list[dict]:
    """Extract decks from DeckUpsertDeckV2 event lines (fallback parser)."""
    result = []
    seen_ids: set[str] = set()

    for line in lines:
        stripped = line.strip()
        if "DeckUpsertDeckV2" not in stripped:
            continue
        # The payload is typically the next line that starts with '{'
        # But in some log formats it's embedded in the same line after the event tag.
        # We scan ahead — for each DeckUpsertDeckV2 marker, look at the next JSON line.

    # Second pass: collect JSON lines that have MainDeck
    for line in lines:
        stripped = line.strip()
        if not stripped.startswith("{") or '"MainDeck"' not in stripped:
            continue
        try:
            data = json.loads(stripped)
        except json.JSONDecodeError:
            continue

        if "Id" not in data or "MainDeck" not in data:
            continue

        deck_id = data.get("Id", "")
        if deck_id in seen_ids:
            continue
        seen_ids.add(deck_id)

        mainboard_raw = data.get("MainDeck", [])
        if not mainboard_raw:
            continue

        mainboard = [
            {"arena_id": int(entry["cardId"]), "quantity": int(entry["quantity"])}
            for entry in mainboard_raw
        ]

        sideboard_raw = data.get("Sideboard", [])
        sideboard = [
            {"arena_id": int(entry["cardId"]), "quantity": int(entry["quantity"])}
            for entry in sideboard_raw
        ]

        result.append({
            "deck_id": deck_id,
            "name": data.get("Name", deck_id),
            "format": "",
            "mainboard": mainboard,
            "sideboard": sideboard,
        })

    return result


def parse_log_decks(log_path: Path) -> list[dict]:
    """Parse Player.log and return all Arena decks found.

    Scans in reverse to find the most recent StartHook event first.
    Falls back to DeckUpsertDeckV2 events if no StartHook is found.

    Args:
        log_path: Path to Player.log

    Returns:
        List of deck dicts:
        [
            {
                "deck_id": str,
                "name": str,
                "format": str,
                "mainboard": [{"arena_id": int, "quantity": int}, ...],
                "sideboard": [{"arena_id": int, "quantity": int}, ...],
            },
            ...
        ]
    """
    with open(log_path, encoding="utf-8", errors="replace") as f:
        lines = f.readlines()

    # Scan reversed lines for most recent StartHook payload
    for line in reversed(lines):
        stripped = line.strip()
        if not stripped.startswith("{"):
            continue
        if '"Decks"' not in stripped or '"InventoryInfo"' not in stripped:
            continue
        try:
            data = json.loads(stripped)
        except json.JSONDecodeError:
            continue
        decks = _parse_starthook(data)
        if decks:
            return decks

    # Fallback: scan DeckUpsertDeckV2 events
    return _parse_upsert_events(lines)
