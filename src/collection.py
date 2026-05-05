"""collection.py — Source DB upsert into the collection table.

Reads card ownership data from the existing mtga_collection.db (written by
the MTGA companion app) and upserts it into the app's collection table.
"""

import json
import sqlite3
from datetime import datetime, date, timezone
from pathlib import Path

from src.db.log_parser import find_player_log, parse_log_wallet

DEFAULT_COLLECTION_PATHS = [
    Path.home() / ".local/share/Steam/steamapps/compatdata/2141910/pfx/drive_c/x.json",
    Path("./x.json"),
]


def find_collection_file(db=None) -> "Path | None":
    """Return the first DEFAULT_COLLECTION_PATHS entry that exists, or None.

    When db is provided, checks the meta table for a saved default path first.

    Args:
        db: Optional sqlite3.Connection. If provided, checks meta table for
            'default_collection_path' before falling back to DEFAULT_COLLECTION_PATHS.
    """
    if db is not None:
        row = db.execute("SELECT value FROM meta WHERE key = 'default_collection_path'").fetchone()
        if row and row["value"]:
            p = Path(row["value"])
            if p.exists():
                return p
    for p in DEFAULT_COLLECTION_PATHS:
        if p.exists():
            return p
    return None


def _snapshot_collection(conn: sqlite3.Connection) -> dict:
    """Return {arena_id: quantity} for all current collection rows."""
    return {
        int(r[0]): int(r[1])
        for r in conn.execute("SELECT arena_id, quantity FROM collection").fetchall()
    }


def _persist_diff(
    conn: sqlite3.Connection,
    old_snap: dict,
    new_snap: dict,
    source: str = "collection",
) -> int:
    """Compute diff between old and new snapshots, persist to DB, return diff count."""
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "INSERT INTO collection_snapshots (snapshot_at, source) VALUES (?, ?)",
        (now, source),
    )
    snap_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    all_ids = set(old_snap) | set(new_snap)
    diffs = []
    for arena_id in all_ids:
        old_q = old_snap.get(arena_id, 0)
        new_q = new_snap.get(arena_id, 0)
        if old_q != new_q:
            diffs.append((snap_id, arena_id, None, old_q, new_q, new_q - old_q))

    if diffs:
        conn.executemany(
            "INSERT INTO collection_snapshot_diffs "
            "(snapshot_id, arena_id, card_name, old_quantity, new_quantity, diff) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            diffs,
        )
    conn.commit()
    return len(diffs)


def upsert_collection(conn: sqlite3.Connection, path: Path, progress_callback=None) -> int:
    """Read arena_id/quantity from source SQLite DB and upsert into collection.

    Opens the source DB at `path`, SELECTs arena_id and quantity from its
    `cards` table, and bulk-upserts those rows into the app's collection table.

    Captures a snapshot of the collection before and after the upsert, then
    persists any changed cards to collection_snapshot_diffs.

    Returns the number of rows attempted (= rows read from source DB).
    Cards whose arena_id is not in the app's cards table are silently skipped
    because PRAGMA foreign_keys is not enabled.
    """
    # 1. Snapshot current state before reload
    old_snap = _snapshot_collection(conn)

    # 2. Read source file — JSON (Untapped x.json) or SQLite (legacy mtga_collection.db)
    now = datetime.now(timezone.utc).isoformat()
    if path.suffix.lower() == ".json":
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        rows = [(int(c["grpid"]), int(c["quantity"]), now) for c in data["cards"]]
    else:
        src = sqlite3.connect(str(path))
        src.row_factory = sqlite3.Row
        try:
            rows_src = src.execute("SELECT arena_id, quantity FROM cards").fetchall()
        finally:
            src.close()
        rows = [(int(r["arena_id"]), int(r["quantity"]), now) for r in rows_src]

    if progress_callback:
        progress_callback("collection", 0, len(rows), f"Loading {len(rows)} collection entries...")
    conn.executemany(
        "INSERT OR REPLACE INTO collection (arena_id, quantity, updated_at) VALUES (?,?,?)",
        rows,
    )
    conn.commit()
    if progress_callback:
        progress_callback("collection", len(rows), len(rows), f"Collection updated ({len(rows)} entries).")

    conn.execute(
        "INSERT OR REPLACE INTO meta VALUES ('collection_last_updated', ?)",
        (now,),
    )
    conn.commit()

    # 3. Snapshot new state and persist diff
    new_snap = _snapshot_collection(conn)
    _persist_diff(conn, old_snap, new_snap)

    # 4. Capture wallet snapshot
    _capture_wallet_snapshot(conn)

    return len(rows)


def _capture_wallet_snapshot(conn: sqlite3.Connection) -> None:
    """Capture current wallet state to wallet_snapshots table.

    Reads wallet data from Player.log and calculates total_cards from collection.
    Uses INSERT OR REPLACE to keep latest value per day.
    """
    today = date.today().isoformat()

    wallet = parse_log_wallet(find_player_log(conn))
    if wallet is None:
        return

    total_cards = conn.execute("""
        SELECT COUNT(DISTINCT LOWER(c.name))
        FROM cards c
        JOIN collection col ON c.arena_id = col.arena_id
        WHERE col.quantity > 0
    """).fetchone()[0] or 0

    conn.execute("""
        INSERT OR REPLACE INTO wallet_snapshots
        (date, gems, gold, mythic_wc, rare_wc, uncommon_wc, common_wc, draft_tokens, total_cards)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        today,
        wallet.get("gems", 0),
        wallet.get("gold", 0),
        wallet.get("mythic_wc", 0),
        wallet.get("rare_wc", 0),
        wallet.get("uncommon_wc", 0),
        wallet.get("common_wc", 0),
        wallet.get("draft_tokens", 0),
        total_cards,
    ))
    conn.commit()
