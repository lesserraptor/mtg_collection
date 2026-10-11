"""Re-run Scryfall enrichment with the fixed Arena-matching on an existing DB.

Repair tool for the booster-clobber bug: paper-only Scryfall printings (promos,
showcase, etc.) that share a name with an MTGA card used to match by name and
overwrite booster / scryfall_id / images with non-Arena values (e.g. pdsk
promos set booster=0 on DSK rares like Valgavoth's Onslaught, arena_id 92296).

This script re-runs ingest_scryfall() — which now only matches printings whose
`games` list contains "arena" — against an EXISTING local bulk cache. It is
fully offline: no network, no card deletion, no collection changes. The Scryfall
cache must already exist (see data/scryfall_all_cards.json). Idempotent: safe to
run twice.

Usage:
    python scripts/fix_scryfall_arena_matching.py [--db PATH] [--scryfall-cache PATH] [--no-backup]
    python -m scripts.fix_scryfall_arena_matching [--db PATH] [--scryfall-cache PATH] [--no-backup]
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

# When run as a script (python scripts/...), the project root is not automatically
# on sys.path. Add it so `src.*` imports resolve correctly (same bootstrap as
# src/db/ingest_cli.py).
_HERE = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(_HERE)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from src.config import DATA_DIR
from src.db.schema import DB_PATH, init_db
from src.db.ingest import (
    ingest_scryfall,
    backfill_rebalanced_images,
    count_missing_scryfall,
)

DEFAULT_CACHE_PATH = DATA_DIR / "scryfall_all_cards.json"


def snapshot_cards(conn: sqlite3.Connection) -> tuple[dict[int, int], dict[int, str]]:
    """Capture booster and scryfall_id per arena_id before enrichment runs."""
    booster = {}
    scryfall_id = {}
    for arena_id, b, sid in conn.execute(
        "SELECT arena_id, booster, scryfall_id FROM cards"
    ):
        booster[arena_id] = b
        scryfall_id[arena_id] = sid
    return booster, scryfall_id


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--db",
        type=Path,
        default=DB_PATH,
        metavar="PATH",
        help=f"SQLite DB path (default: {DB_PATH})",
    )
    parser.add_argument(
        "--scryfall-cache",
        type=Path,
        default=DEFAULT_CACHE_PATH,
        metavar="PATH",
        help=f"Scryfall bulk cache path (default: {DEFAULT_CACHE_PATH})",
    )
    parser.add_argument(
        "--no-backup",
        action="store_true",
        help="Skip creating a .bak-<timestamp> backup of the DB before writing",
    )
    args = parser.parse_args()

    db_path: Path = args.db
    cache_path: Path = args.scryfall_cache

    if not cache_path.exists():
        print(
            f"ERROR: Scryfall cache not found at {cache_path}\n"
            "       Run the full ingest (python src/db/ingest_cli.py) once to "
            "download it, or pass --scryfall-cache.",
            file=sys.stderr,
        )
        return 1

    if not db_path.exists():
        print(f"ERROR: DB not found at {db_path}", file=sys.stderr)
        return 1

    # Backup the DB before any writes unless explicitly disabled.
    if not args.no_backup:
        backup_path = db_path.with_name(
            f"{db_path.name}.bak-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
        )
        src = sqlite3.connect(str(db_path))
        dest = sqlite3.connect(str(backup_path))
        try:
            src.backup(dest)
        finally:
            dest.close()
            src.close()
        print(f"Backed up DB to {backup_path}", file=sys.stderr)
    else:
        print("Skipping backup (--no-backup).", file=sys.stderr)

    conn = init_db(db_path)

    # Build the fallback match maps exactly as ingest_cli.py does.
    rows = conn.execute(
        "SELECT arena_id, name, set_code, collector_number FROM cards"
    ).fetchall()
    arena_id_map: dict = {}
    name_set_map: dict = {}
    name_counts: dict = {}
    src_name_by_id: dict = {}
    for arena_id, name, set_code, cn in rows:
        src_name_by_id[arena_id] = name
        if set_code and cn:
            arena_id_map[(set_code.lower(), str(cn))] = arena_id
        if name and set_code:
            name_set_map[(name, set_code.lower())] = arena_id
        if name:
            name_counts[name] = name_counts.get(name, 0) + 1
    name_map = {}
    for (name, set_code), arena_id in name_set_map.items():
        if name_counts.get(name, 0) == 1:
            name_map[name] = arena_id

    rebalanced_id_map: dict = {}
    for arena_id, set_code, cn in conn.execute(
        "SELECT arena_id, set_code, collector_number FROM cards WHERE is_rebalanced=1"
    ):
        if set_code and cn:
            rebalanced_id_map[(set_code.lower(), str(cn))] = arena_id

    # Map arena_id -> (set_code_lower, collector_number) so Scryfall can reject
    # same-set variant printings (e.g. Starter Collection reprints) that share a
    # name with the base card but have a different collector number.
    src_setcn_by_id: dict = {
        row[0]: (row[1].lower(), str(row[2]))
        for row in conn.execute("SELECT arena_id, set_code, collector_number FROM cards")
        if row[1] and row[2]
    }

    # BEFORE stats.
    before_booster, before_scryfall = snapshot_cards(conn)
    before_total = len(before_booster)
    before_zero = sum(1 for b in before_booster.values() if b == 0)
    before_missing = count_missing_scryfall(conn)
    print(
        f"BEFORE: total={before_total} booster=0={before_zero} "
        f"missing_scryfall={before_missing}",
        file=sys.stderr,
    )

    # Re-run enrichment (fixed matching) against the existing cache, offline.
    print("Re-running Scryfall enrichment (fixed Arena matching)...", file=sys.stderr)
    processed = ingest_scryfall(
        cache_path, conn,
        arena_id_map=arena_id_map,
        name_set_map=name_set_map,
        name_map=name_map,
        src_name_by_id=src_name_by_id,
        rebalanced_id_map=rebalanced_id_map,
        src_setcn_by_id=src_setcn_by_id,
    )
    print(f"Processed {processed} Scryfall Arena cards.", file=sys.stderr)

    backfilled = backfill_rebalanced_images(conn)
    print(f"Backfilled images for {backfilled} rebalanced cards.", file=sys.stderr)

    # AFTER stats.
    after_booster, after_scryfall = snapshot_cards(conn)
    after_total = len(after_booster)
    after_zero = sum(1 for b in after_booster.values() if b == 0)
    after_missing = count_missing_scryfall(conn)

    # Compare snapshots.
    flips_0to1 = [
        aid for aid in after_booster
        if before_booster.get(aid) == 0 and after_booster.get(aid) == 1
    ]
    flips_1to0 = [
        aid for aid in after_booster
        if before_booster.get(aid) == 1 and after_booster.get(aid) == 0
    ]
    scryfall_changes = [
        aid for aid in after_scryfall
        if after_scryfall.get(aid) != before_scryfall.get(aid)
    ]

    # Verification row: Valgavoth's Onslaught (DSK rare, arena_id 92296).
    verify = conn.execute(
        "SELECT booster, scryfall_id, name FROM cards WHERE arena_id = 92296"
    ).fetchone()
    v_name = verify["name"] if verify else "?"
    v_booster = verify["booster"] if verify else None
    v_sid = verify["scryfall_id"] if verify else None
    v_before = before_booster.get(92296)
    v_sid_before = before_scryfall.get(92296)

    print()
    print("=== Fix summary ===")
    print(f"  booster flips 0->1:  {len(flips_0to1)}")
    print(f"  booster flips 1->0:  {len(flips_1to0)}")
    print(f"  scryfall_id changes: {len(scryfall_changes)}")
    print(f"  booster=0 before:    {before_zero}")
    print(f"  booster=0 after:     {after_zero}")
    print(f"  missing_scryfall:    {before_missing} -> {after_missing}")
    print(
        f"  verify 92296 ({v_name}): booster {v_before} -> {v_booster}, "
        f"scryfall_id {v_sid_before} -> {v_sid}"
    )
    print(f"  DB path:             {db_path}")

    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())