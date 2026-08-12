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

# UPSERT by arena_id. Updates ONLY native CardDB fields; Scryfall enrichment
# fields (scryfall_id, cmc, type_line, oracle_text, colors, color_identity,
# keywords, layout, image_uri_front, image_uri_back) are never touched here.
# is_active=1 on every row seen in this CardDB load (base set = MTGA only).
_UPSERT_SQL = """
    INSERT INTO cards (
        arena_id, scryfall_id, name, mana_cost, rarity, set_code,
        collector_number, is_rebalanced, is_active
    ) VALUES (?,?,?,?,?,?,?,?,?)
    ON CONFLICT(arena_id) DO UPDATE SET
        name             = excluded.name,
        mana_cost        = excluded.mana_cost,
        rarity           = excluded.rarity,
        set_code         = excluded.set_code,
        collector_number = excluded.collector_number,
        is_rebalanced    = excluded.is_rebalanced,
        is_active        = 1
"""

_BATCH_SIZE = 500


def ingest_mtga_card_db(conn: sqlite3.Connection, card_db_path: Path, progress_callback=None) -> int:
    """Read native MTGA CardDB and upsert IsPrimaryCard=1 cards into app DB.

    The MTGA CardDatabase is the authoritative BASE SET. This function UPSERTs
    by arena_id, updating only native fields (name, mana_cost, rarity,
    set_code, collector_number, is_rebalanced) and setting is_active=1.
    Scryfall enrichment fields (scryfall_id, cmc, type_line, oracle_text,
    colors, color_identity, keywords, layout, image_uri_front, image_uri_back)
    are never clobbered here — Scryfall owns them.

    Renamed cards: before upserting, existing (arena_id -> name) rows are
    read. If a row's name CHANGED vs the old value, its enrichment fields are
    cleared so the next Scryfall refresh re-enriches cleanly under the new
    name.

    After processing all CardDB rows, any card NOT present in this CardDB load
    (cards that left MTGA, or legacy scryfall-only rows) is marked is_active=0.
    Rows are never deleted — collection/decks FK-reference cards with no
    cascade.

    Opens card_db_path as a read-only SQLite connection (uri=True with
    ?mode=ro). Joins Cards with Localizations_enUS to get English card names.

    Args:
        conn: Open sqlite3.Connection to the app database (cards table must exist).
        card_db_path: Path to the Raw_CardDatabase_*.mtga file.

    Returns:
        Total number of rows upserted (inserted or updated).
    """
    card_db_uri = Path(card_db_path).as_uri() + "?mode=ro"
    src_conn = sqlite3.connect(card_db_uri, uri=True)
    src_conn.row_factory = sqlite3.Row

    try:
        # Existing names by arena_id, so renamed cards can be detected and have
        # their Scryfall enrichment cleared for a clean re-enrich.
        existing_names = dict(conn.execute("SELECT arena_id, name FROM cards"))
        renamed_ids: set[int] = set()
        seen_ids: set[int] = set()
        batch = []

        cursor = src_conn.execute(_QUERY)
        for row in cursor:
            name = re.sub(r"<[^>]+>", "", row["name"])
            arena_id = row["arena_id"]
            seen_ids.add(arena_id)
            if arena_id in existing_names and existing_names[arena_id] != name:
                renamed_ids.add(arena_id)
            tuple_row = (
                arena_id,
                "",                                                          # scryfall_id: placeholder, never updated on conflict
                name,
                _decode_mana(row["raw_mana"] or ""),                         # mana_cost
                RARITY_MAP.get(row["rarity_id"], "common"),                  # rarity
                row["set_code"].lower() if row["set_code"] else "",          # set_code
                str(row["collector_number"]) if row["collector_number"] else None,  # collector_number
                int(row["is_rebalanced"]),
                1,                                                          # is_active: present in this CardDB load
            )
            batch.append(tuple_row)

            if len(batch) >= _BATCH_SIZE:
                conn.executemany(_UPSERT_SQL, batch)
                conn.commit()
                if progress_callback:
                    # MTGA card DB has ~18k cards; estimate pct accordingly
                    approx_done = len(seen_ids)
                    pct = min(int(approx_done / 18000 * 100), 99)
                    progress_callback("ingest", approx_done, 18000, f"Ingesting cards ({approx_done} processed)...")
                batch = []

        if batch:
            conn.executemany(_UPSERT_SQL, batch)
            conn.commit()

        total = len(seen_ids)

        # Clear enrichment for renamed cards so the next Scryfall refresh
        # re-enriches them cleanly (old data may describe a different card).
        if renamed_ids:
            id_list = list(renamed_ids)
            for i in range(0, len(id_list), _BATCH_SIZE):
                chunk = id_list[i:i + _BATCH_SIZE]
                placeholders = ",".join("?" * len(chunk))
                conn.execute(f"""
                    UPDATE cards SET
                        scryfall_id='', cmc=NULL, type_line=NULL, oracle_text=NULL,
                        colors='[]', color_identity='[]', keywords='[]',
                        layout=NULL, image_uri_front=NULL, image_uri_back=NULL
                    WHERE arena_id IN ({placeholders})
                """, chunk)
            conn.commit()

        # Mark stale: rows not seen in this CardDB load left the base set
        # (removed from MTGA, or legacy scryfall-only rows). Never delete.
        conn.execute("CREATE TEMP TABLE _carddb_seen (arena_id INTEGER PRIMARY KEY)")
        conn.executemany(
            "INSERT OR IGNORE INTO _carddb_seen (arena_id) VALUES (?)",
            [(aid,) for aid in seen_ids],
        )
        cursor = conn.execute("""
            UPDATE cards SET is_active = 0
            WHERE arena_id NOT IN (SELECT arena_id FROM _carddb_seen)
        """)
        inactive_count = cursor.rowcount
        conn.execute("DROP TABLE _carddb_seen")
        conn.commit()
        if progress_callback:
            progress_callback(
                "ingest", len(seen_ids), 18000,
                f"Marked {inactive_count} stale card(s) inactive.",
            )

    finally:
        src_conn.close()

    return total
