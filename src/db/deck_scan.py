"""Deck scan logic: diff log vs DB, produce ScanResult, apply versions.

Phase 15: stubs only. Phase 16 implements scan_log_decks() and apply_scan_result().
"""
from __future__ import annotations

import hashlib
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.db.log_parser import parse_log_decks


@dataclass
class ParsedDeck:
    log_deck_id: str
    name: str
    format: str
    mainboard: list[dict]  # [{"arena_id": int, "quantity": int}, ...]
    sideboard: list[dict]
    commander: list[dict] = field(default_factory=list)


@dataclass
class ChangedDeck:
    deck_id: int
    name: str
    log_deck_id: str
    old_hash: str
    new_hash: str
    parsed: ParsedDeck


@dataclass
class MissingDeck:
    deck_id: int
    name: str
    log_deck_id: str


@dataclass
class ScanResult:
    new_decks: list[ParsedDeck] = field(default_factory=list)
    changed_decks: list[ChangedDeck] = field(default_factory=list)
    missing_decks: list[MissingDeck] = field(default_factory=list)


def compute_content_hash(mainboard: list[dict]) -> str:
    """Compute a stable SHA-256 hash for a deck's mainboard.

    Input: list of {"arena_id": int, "quantity": int} dicts.
    Output: hex digest string.
    Stability: sort by arena_id before hashing so order doesn't matter.
    """
    sorted_pairs = sorted(
        (entry["arena_id"], entry["quantity"]) for entry in mainboard
    )
    payload = ",".join(f"{aid}:{qty}" for aid, qty in sorted_pairs)
    return hashlib.sha256(payload.encode()).hexdigest()


def scan_log_decks(db: sqlite3.Connection, log_path: Path) -> ScanResult:
    """Parse log and diff against DB. Returns ScanResult with new/changed/missing buckets.

    Phase 16 implementation: compares parsed decks against decks WHERE source='log'
    using log_deck_id for stable identity and content_hash for O(1) change detection.
    """
    raw_decks = parse_log_decks(log_path)

    # Build parsed index keyed by MTGA deck GUID
    parsed_index: dict[str, ParsedDeck] = {}
    for raw in raw_decks:
        parsed_index[raw["deck_id"]] = ParsedDeck(
            log_deck_id=raw["deck_id"],
            name=raw["name"],
            format=raw.get("format") or "",
            mainboard=raw["mainboard"],
            sideboard=raw["sideboard"],
            commander=raw.get("commander", []),
        )

    # Load current DB state (source='log' rows only)
    db_rows = db.execute(
        "SELECT id, name, log_deck_id, content_hash, scan_status FROM decks WHERE source='log'"
    ).fetchall()
    db_index: dict[str, sqlite3.Row] = {
        row["log_deck_id"]: row for row in db_rows if row["log_deck_id"]
    }

    result = ScanResult()

    # Detect new and changed
    for log_deck_id, parsed in parsed_index.items():
        if log_deck_id not in db_index:
            result.new_decks.append(parsed)
        else:
            db_row = db_index[log_deck_id]
            new_hash = compute_content_hash(parsed.mainboard)
            if new_hash != db_row["content_hash"]:
                result.changed_decks.append(ChangedDeck(
                    deck_id=db_row["id"],
                    name=db_row["name"],
                    log_deck_id=log_deck_id,
                    old_hash=db_row["content_hash"] or "",
                    new_hash=new_hash,
                    parsed=parsed,
                ))

    # Detect missing (exclude explicitly ignored)
    for log_deck_id, db_row in db_index.items():
        if db_row["scan_status"] == "missing_ignore":
            continue
        if log_deck_id not in parsed_index:
            result.missing_decks.append(MissingDeck(
                deck_id=db_row["id"],
                name=db_row["name"],
                log_deck_id=log_deck_id,
            ))

    return result


def create_version(db: sqlite3.Connection, deck_id: int) -> int:
    """Snapshot current deck_lines into deck_versions + deck_version_lines.

    Returns version_id of the new snapshot.
    Phase 16 implementation.
    """
    row = db.execute(
        "SELECT MAX(version_num) AS mx FROM deck_versions WHERE deck_id = ?",
        (deck_id,)
    ).fetchone()
    next_num = (row["mx"] or 0) + 1

    now = datetime.now(timezone.utc).isoformat()
    cursor = db.execute(
        "INSERT INTO deck_versions (deck_id, version_num, label, created_at, source) VALUES (?, ?, NULL, ?, 'scan')",
        (deck_id, next_num, now)
    )
    version_id = cursor.lastrowid

    db.execute(
        """INSERT INTO deck_version_lines (version_id, arena_id, card_name, quantity, section)
           SELECT ?, arena_id, card_name, quantity, section
           FROM deck_lines WHERE deck_id = ?""",
        (version_id, deck_id)
    )
    return version_id


def apply_scan_result(db: sqlite3.Connection, result: ScanResult) -> None:
    """Apply auto-actions from a ScanResult: import new decks, snapshot changed decks.

    Changed/missing decks require user decisions (handled by bulk dialog in Phase 17).
    New decks are auto-imported with log_deck_id + content_hash patched.
    Changed decks get a version snapshot + content_hash update (lines preserved for Phase 17).
    Missing decks are stored in caller's app.state.pending_scan — no DB writes here.
    Do NOT call db.commit() inside this function — caller commits.
    Phase 16 implementation.
    """
    from src.db.decks import _build_arena_text_from_log_deck, import_deck

    # Auto-import new decks
    for parsed in result.new_decks:
        arena_text = _build_arena_text_from_log_deck(db, {
            "name": parsed.name,
            "mainboard": parsed.mainboard,
            "sideboard": parsed.sideboard,
            "commander": parsed.commander,
            "format": parsed.format,
        })
        if arena_text is None:
            continue
        deck_id = import_deck(db, text=arena_text, is_potential=False, is_saved=False)
        db.execute(
            "UPDATE decks SET source='log', format=?, log_deck_id=?, content_hash=? WHERE id=?",
            (parsed.format or None, parsed.log_deck_id, compute_content_hash(parsed.mainboard), deck_id)
        )

    # Snapshot changed decks and update content_hash
    for changed in result.changed_decks:
        create_version(db, changed.deck_id)
        db.execute(
            "UPDATE decks SET content_hash=? WHERE id=?",
            (changed.new_hash, changed.deck_id)
        )
        # Do NOT overwrite deck_lines — Phase 17 handles user decision

    # Missing decks: no DB writes in Phase 16
    # Caller stores result in app.state.pending_scan


def apply_changed_deck_lines(db: sqlite3.Connection, deck_id: int, parsed: ParsedDeck) -> None:
    """Replace deck_lines for deck_id with content from parsed.

    DELETEs all existing deck_lines rows for deck_id, then INSERTs rows from
    parsed.mainboard, parsed.sideboard, and parsed.commander.

    card_name is resolved via cards table lookup by arena_id; falls back to '' if not found.
    Does NOT call db.commit() — caller commits after all decisions are applied.
    """
    db.execute("DELETE FROM deck_lines WHERE deck_id = ?", (deck_id,))

    sections = [
        ("mainboard", parsed.mainboard),
        ("sideboard", parsed.sideboard),
        ("commander", parsed.commander),
    ]

    for section, entries in sections:
        for entry in entries:
            arena_id = entry["arena_id"]
            quantity = entry["quantity"]
            row = db.execute(
                "SELECT name FROM cards WHERE arena_id = ?", (arena_id,)
            ).fetchone()
            card_name = row["name"] if row else ""
            db.execute(
                "INSERT INTO deck_lines (deck_id, arena_id, card_name, quantity, section) VALUES (?, ?, ?, ?, ?)",
                (deck_id, arena_id, card_name, quantity, section),
            )
