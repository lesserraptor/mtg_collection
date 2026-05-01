"""SQLite schema module for the MTGA collection browser.

Provides open_db(), create_schema(), create_indexes(), and init_db()
for all DDL, PRAGMA setup, and index creation.
"""

import sqlite3
from pathlib import Path

from src.config import DATA_DIR

DB_PATH = DATA_DIR / "mtga_collection.db"


def open_db(db_path=None) -> sqlite3.Connection:
    """Open (or create) a SQLite database with performance PRAGMAs set.

    Args:
        db_path: Path to the database file. Defaults to DB_PATH (data/mtga_collection.db).

    Returns:
        sqlite3.Connection with row_factory set to sqlite3.Row.
    """
    if db_path is None:
        db_path = DB_PATH

    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    conn.executescript("""
        PRAGMA journal_mode = WAL;
        PRAGMA synchronous = NORMAL;
        PRAGMA cache_size = -65536;
        PRAGMA temp_store = MEMORY;
        PRAGMA mmap_size = 536870912;
    """)
    conn.row_factory = sqlite3.Row
    return conn


def create_schema(conn: sqlite3.Connection) -> None:
    """Create all tables using IF NOT EXISTS DDL.

    Tables: cards, collection, decks, deck_lines, meta.
    """
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS cards (
            arena_id         INTEGER PRIMARY KEY,
            scryfall_id      TEXT    NOT NULL,
            name             TEXT    NOT NULL,
            mana_cost        TEXT,
            cmc              REAL,
            type_line        TEXT,
            oracle_text      TEXT,
            rarity           TEXT    NOT NULL,
            set_code         TEXT    NOT NULL,
            collector_number TEXT,
            colors           TEXT,
            color_identity   TEXT,
            keywords         TEXT,
            layout           TEXT,
            image_uri_front  TEXT,
            image_uri_back   TEXT,
            is_rebalanced    INTEGER NOT NULL DEFAULT 0,
            booster          INTEGER NOT NULL DEFAULT 1
        );

        CREATE TABLE IF NOT EXISTS collection (
            arena_id   INTEGER PRIMARY KEY REFERENCES cards(arena_id),
            quantity   INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT    NOT NULL
        );

        CREATE TABLE IF NOT EXISTS decks (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            name        TEXT    NOT NULL,
            format      TEXT,
            imported_at TEXT    NOT NULL,
            source      TEXT
        );
    """)
    # Migration: add is_potential column (safe to run on existing DBs)
    try:
        conn.execute(
            "ALTER TABLE decks ADD COLUMN is_potential INTEGER NOT NULL DEFAULT 0"
        )
        conn.commit()
    except sqlite3.OperationalError:
        pass  # Column already exists — safe to ignore
    # Migration: add is_saved column (safe to run on existing DBs)
    try:
        conn.execute(
            "ALTER TABLE decks ADD COLUMN is_saved INTEGER NOT NULL DEFAULT 0"
        )
        conn.commit()
    except sqlite3.OperationalError:
        pass  # Column already exists — safe to ignore
    # Migrations: add v4.0 deck scan columns (safe to run on existing DBs)
    for col_ddl in [
        "ALTER TABLE decks ADD COLUMN log_deck_id TEXT",
        "ALTER TABLE decks ADD COLUMN content_hash TEXT",
        "ALTER TABLE decks ADD COLUMN scan_status TEXT NOT NULL DEFAULT 'active'",
    ]:
        try:
            conn.execute(col_ddl)
            conn.commit()
        except sqlite3.OperationalError:
            pass  # Column already exists
    # Migration: add cards.booster column (1 = appears in booster packs, 0 = does not).
    # Default 1 preserves prior behavior for rows enrichment has not revisited yet;
    # Scryfall ingest overwrites with the real value on the next refresh.
    try:
        conn.execute("ALTER TABLE cards ADD COLUMN booster INTEGER NOT NULL DEFAULT 1")
        conn.commit()
    except sqlite3.OperationalError:
        pass  # Column already exists
    conn.executescript("""

        CREATE TABLE IF NOT EXISTS deck_lines (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            deck_id   INTEGER NOT NULL REFERENCES decks(id) ON DELETE CASCADE,
            arena_id  INTEGER REFERENCES cards(arena_id),
            card_name TEXT    NOT NULL,
            quantity  INTEGER NOT NULL DEFAULT 1,
            section   TEXT    NOT NULL DEFAULT 'mainboard'
        );

        CREATE TABLE IF NOT EXISTS meta (
            key   TEXT PRIMARY KEY,
            value TEXT
        );

        CREATE TABLE IF NOT EXISTS collection_snapshots (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            snapshot_at TEXT    NOT NULL,
            source      TEXT    NOT NULL DEFAULT 'collection'
        );

        CREATE TABLE IF NOT EXISTS collection_snapshot_diffs (
            snapshot_id  INTEGER NOT NULL REFERENCES collection_snapshots(id) ON DELETE CASCADE,
            arena_id     INTEGER NOT NULL,
            card_name    TEXT,
            old_quantity INTEGER NOT NULL DEFAULT 0,
            new_quantity INTEGER NOT NULL DEFAULT 0,
            diff         INTEGER NOT NULL,
            PRIMARY KEY (snapshot_id, arena_id)
        );

        CREATE TABLE IF NOT EXISTS errata (
            arena_id  INTEGER NOT NULL,
            field     TEXT    NOT NULL,
            new_value TEXT,
            note      TEXT,
            PRIMARY KEY (arena_id, field)
        );

        CREATE TABLE IF NOT EXISTS deck_versions (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            deck_id     INTEGER NOT NULL REFERENCES decks(id) ON DELETE CASCADE,
            version_num INTEGER NOT NULL,
            label       TEXT,
            created_at  TEXT    NOT NULL,
            source      TEXT    NOT NULL DEFAULT 'scan',
            UNIQUE(deck_id, version_num)
        );

        CREATE TABLE IF NOT EXISTS deck_version_lines (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            version_id  INTEGER NOT NULL REFERENCES deck_versions(id) ON DELETE CASCADE,
            arena_id    INTEGER REFERENCES cards(arena_id),
            card_name   TEXT    NOT NULL,
            quantity    INTEGER NOT NULL DEFAULT 1,
            section     TEXT    NOT NULL DEFAULT 'mainboard'
        );
    """)


def create_indexes(conn: sqlite3.Connection) -> None:
    """Create all performance indexes on the cards and deck_lines tables."""
    conn.executescript("""
        CREATE INDEX IF NOT EXISTS idx_cards_name         ON cards(name);
        CREATE INDEX IF NOT EXISTS idx_cards_rarity       ON cards(rarity);
        CREATE INDEX IF NOT EXISTS idx_cards_set_code     ON cards(set_code);
        CREATE INDEX IF NOT EXISTS idx_cards_cmc          ON cards(cmc);
        CREATE INDEX IF NOT EXISTS idx_cards_colors       ON cards(colors);
        CREATE INDEX IF NOT EXISTS idx_cards_type_line    ON cards(type_line);
        CREATE INDEX IF NOT EXISTS idx_deck_lines_deck_id ON deck_lines(deck_id);
        CREATE INDEX IF NOT EXISTS idx_deck_lines_arena_id ON deck_lines(arena_id);
        CREATE INDEX IF NOT EXISTS idx_decks_is_potential ON decks(is_potential);
        CREATE INDEX IF NOT EXISTS idx_deck_versions_deck_id ON deck_versions(deck_id);
        CREATE INDEX IF NOT EXISTS idx_deck_version_lines_version_id ON deck_version_lines(version_id);
        CREATE INDEX IF NOT EXISTS idx_decks_log_deck_id ON decks(log_deck_id);
        CREATE INDEX IF NOT EXISTS idx_decks_scan_status ON decks(scan_status);
        CREATE INDEX IF NOT EXISTS idx_diffs_snapshot_id
            ON collection_snapshot_diffs(snapshot_id);
        CREATE INDEX IF NOT EXISTS idx_snapshots_snapshot_at
            ON collection_snapshots(snapshot_at DESC);
    """)
    conn.commit()


def init_db(db_path=None) -> sqlite3.Connection:
    """Open the database and ensure schema and indexes exist.

    Convenience function: calls open_db(), create_schema(), create_indexes().

    Args:
        db_path: Path to the database file. Defaults to DB_PATH.

    Returns:
        sqlite3.Connection ready for use.
    """
    conn = open_db(db_path)
    create_schema(conn)
    create_indexes(conn)
    return conn
