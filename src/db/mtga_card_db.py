"""Native MTGA CardDB reader pipeline.

This module provides the primary card catalog ingestion pipeline by reading
Raw_CardDatabase_*.mtga (a SQLite database shipped with MTGA) and populating
the app's cards table with authoritative arena_id, name, set_code,
collector_number, rarity, and mana_cost values.

Scryfall enrichment (ingest.py) fills the remaining fields: scryfall_id,
cmc, type_line, oracle_text, colors, color_identity, keywords, layout,
image_uri_front, image_uri_back.
"""

import glob
import os
import re
import sqlite3
from pathlib import Path

from src.config import DATA_DIR

RARITY_MAP = {0: "token", 1: "basic", 2: "common", 3: "uncommon", 4: "rare", 5: "mythic"}

CARD_DB_GLOBS = [
    # Configurable via env var (for dev: point at project data/ dir)
    os.environ.get("MTGA_CARD_DB_PATH", ""),
    # Project data/ dir — place Raw_CardDatabase.mtga here for portability
    str(DATA_DIR / "Raw_CardDatabase.mtga"),
    # Linux / Steam (Proton) — production path when app runs on the Linux machine
    str(Path.home() / ".local/share/Steam/steamapps/common/MTGA/MTGA_Data/Downloads/Raw/Raw_CardDatabase_*.mtga"),
    # macOS
    str(Path.home() / "Library/Application Support/com.wizards.mtga/Downloads/Raw/Raw_CardDatabase_*.mtga"),
]


def find_card_db(db=None) -> Path | None:
    """Locate the native MTGA Raw_CardDatabase_*.mtga file.

    When db is provided, checks the meta table for a saved default path first.
    Falls back to CARD_DB_GLOBS in order. For each entry, skips empty strings,
    then uses glob.glob() and returns the first match found.

    Args:
        db: Optional sqlite3.Connection. If provided, checks meta table for
            'default_card_db_path' before falling back to CARD_DB_GLOBS.

    Returns:
        Path to the first matching CardDB file, or None if not found.
    """
    if db is not None:
        row = db.execute("SELECT value FROM meta WHERE key = 'default_card_db_path'").fetchone()
        if row and row["value"]:
            p = Path(row["value"])
            if p.exists():
                return p
    for pattern in CARD_DB_GLOBS:
        if not pattern:
            continue
        matches = glob.glob(pattern)
        if matches:
            return Path(matches[0]).resolve()
    return None


def _decode_mana(s: str) -> str:
    """Decode MTGA native mana cost format to standard notation.

    Converts the internal format (e.g. 'o2oWoU') to standard notation
    (e.g. '{2}{W}{U}'). Each segment starting with 'o' is replaced: the
    character after 'o' becomes '{char}'.

    Args:
        s: Raw mana string from OldSchoolManaText column.

    Returns:
        Decoded mana cost string, or "" if s is empty or None.
    """
    if not s:
        return ""
    result = []
    i = 0
    while i < len(s):
        if s[i] == "o" and i + 1 < len(s):
            result.append("{" + s[i + 1] + "}")
            i += 2
        else:
            result.append(s[i])
            i += 1
    return "".join(result)


_QUERY = """
SELECT c.GrpId as arena_id,
       l.Loc   as name,
       c.ExpansionCode as set_code,
       c.CollectorNumber as collector_number,
       c.Rarity as rarity_id,
       c.IsRebalanced as is_rebalanced,
       c.OldSchoolManaText as raw_mana,
       c.Types as type_ids,
       c.Colors as color_ids
FROM Cards c
JOIN Localizations_enUS l ON c.TitleId = l.LocId AND l.Formatted = 1
WHERE c.IsPrimaryCard = 1
"""

_INSERT_SQL = """
    INSERT OR IGNORE INTO cards (
        arena_id, scryfall_id, name, mana_cost, cmc, type_line, oracle_text,
        rarity, set_code, collector_number, colors, color_identity, keywords,
        layout, image_uri_front, image_uri_back, is_rebalanced
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
"""

_BATCH_SIZE = 500


def ingest_mtga_card_db(conn: sqlite3.Connection, card_db_path: Path, progress_callback=None) -> int:
    """Read native MTGA CardDB and insert IsPrimaryCard=1 cards into app DB.

    Opens card_db_path as a read-only SQLite connection (uri=True with
    ?mode=ro). Joins Cards with Localizations_enUS to get English card names.
    Uses INSERT OR IGNORE so subsequent Scryfall enrichment cannot overwrite
    native CardDB entries.

    Args:
        conn: Open sqlite3.Connection to the app database (cards table must exist).
        card_db_path: Path to the Raw_CardDatabase_*.mtga file.

    Returns:
        Total number of rows inserted (0 if all already present due to OR IGNORE).
    """
    card_db_uri = Path(card_db_path).as_uri() + "?mode=ro"
    src_conn = sqlite3.connect(card_db_uri, uri=True)
    src_conn.row_factory = sqlite3.Row

    try:
        cursor = src_conn.execute(_QUERY)
        batch = []
        changes_before = conn.total_changes

        for row in cursor:
            name = re.sub(r"<[^>]+>", "", row["name"])
            tuple_row = (
                row["arena_id"],
                "",                                                          # scryfall_id: placeholder
                name,
                _decode_mana(row["raw_mana"] or ""),                         # mana_cost
                None,                                                        # cmc: Scryfall fills
                None,                                                        # type_line: Scryfall fills
                None,                                                        # oracle_text: Scryfall fills
                RARITY_MAP.get(row["rarity_id"], "common"),                  # rarity
                row["set_code"].lower() if row["set_code"] else "",          # set_code
                str(row["collector_number"]) if row["collector_number"] else None,  # collector_number
                "[]",                                                        # colors: Scryfall fills
                "[]",                                                        # color_identity: Scryfall fills
                "[]",                                                        # keywords: Scryfall fills
                None,                                                        # layout: Scryfall fills
                None,                                                        # image_uri_front: Scryfall fills
                None,                                                        # image_uri_back: Scryfall fills
                int(row["is_rebalanced"]),
            )
            batch.append(tuple_row)

            if len(batch) >= _BATCH_SIZE:
                conn.executemany(_INSERT_SQL, batch)
                conn.commit()
                if progress_callback:
                    # MTGA card DB has ~18k cards; estimate pct accordingly
                    approx_done = conn.total_changes - changes_before
                    pct = min(int(approx_done / 18000 * 100), 99)
                    progress_callback("ingest", approx_done, 18000, f"Ingesting cards ({approx_done} processed)...")
                batch = []

        if batch:
            conn.executemany(_INSERT_SQL, batch)
            conn.commit()

        total = conn.total_changes - changes_before

    finally:
        src_conn.close()

    return total
