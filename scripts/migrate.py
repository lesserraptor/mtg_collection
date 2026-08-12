"""migrate.py — standalone, idempotent schema migrations.

Usage:
    python scripts/migrate.py [path/to.db]

Defaults to data/mtga_collection.db. Safe to re-run; never destructive.
Records the current schema version in the meta table ('schema_version').

Version history:
    1 — original schema
    2 — cards.is_active column (1 = in current MTGA CardDB, 0 = stale)
"""

import argparse
import os
import sqlite3
import sys

# When run as a script (python scripts/migrate.py), the project root is not
# automatically on sys.path. Add it so `src.*` imports resolve correctly.
_HERE = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(_HERE)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from src.db.schema import DB_PATH

SCHEMA_VERSION = "2"


def migrate(db_path) -> list[str]:
    """Apply pending migrations to db_path; returns list of human-readable actions.

    Idempotent: safe to call repeatedly against the same DB. Never destructive —
    only ALTER TABLE ADD COLUMN and meta bookkeeping.
    """
    actions: list[str] = []
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT)")
        conn.commit()

        row = conn.execute(
            "SELECT value FROM meta WHERE key = 'schema_version'"
        ).fetchone()
        current = int(row[0]) if row and str(row[0]).isdigit() else 1

        if current >= int(SCHEMA_VERSION):
            actions.append(f"Already at schema_version {current} — nothing to do.")
            return actions

        # v2: cards.is_active
        cards_table = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='cards'"
        ).fetchone()
        if cards_table is None:
            actions.append(
                "ERROR: cards table not found — run the app once (init_db) to create "
                "the schema first, or point this at the correct DB file."
            )
            return actions

        cols = [r[1] for r in conn.execute("PRAGMA table_info(cards)")]
        if "is_active" in cols:
            actions.append("cards.is_active already present — skipped ALTER.")
        else:
            conn.execute(
                "ALTER TABLE cards ADD COLUMN is_active INTEGER NOT NULL DEFAULT 1"
            )
            actions.append("Added cards.is_active (INTEGER NOT NULL DEFAULT 1).")

        conn.execute(
            "INSERT OR REPLACE INTO meta (key, value) VALUES ('schema_version', ?)",
            (SCHEMA_VERSION,),
        )
        conn.commit()
        actions.append(f"Recorded schema_version = {SCHEMA_VERSION} in meta.")
    finally:
        conn.close()
    return actions


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Apply pending schema migrations to an MTGA collection DB."
    )
    parser.add_argument(
        "db_path",
        nargs="?",
        default=str(DB_PATH),
        help=f"SQLite DB path (default: {DB_PATH})",
    )
    args = parser.parse_args()
    print(f"Migrating: {args.db_path}")
    for action in migrate(args.db_path):
        print(f"  {action}")


if __name__ == "__main__":
    main()
