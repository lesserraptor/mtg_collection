"""Backfill the cards.booster column from the cached Scryfall bulk data.

Use this after adopting the booster-aware set-completion logic on a DB whose
rows predate the new column: it streams data/scryfall_all_cards.json and
sets cards.booster = 0 for every printing Scryfall flags as non-booster,
matching first by arena_id and falling back to (set_code, collector_number).

Only cards that appear in the cache are touched, so rows without a Scryfall
match keep the default value (1). The script is idempotent. Clicking Refresh
Scryfall in the Settings page does the same work (plus the full enrichment
pass) — this script exists for cases where a targeted booster-only backfill
is preferable to a 2.4 GB re-ingest.

Usage:
    python -m scripts.backfill_booster [--cache PATH] [--db PATH]
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import ijson

from src.config import DATA_DIR
from src.db.schema import init_db


def backfill_booster(cache_path: Path, db_path: Path | None = None) -> tuple[int, int]:
    """Return (rows_set_to_zero_by_arena_id, rows_set_to_zero_by_set_cn)."""
    conn = init_db(db_path)

    # Reset booster to its default before scanning so stale zeros from a prior
    # incomplete backfill don't survive.
    conn.execute("UPDATE cards SET booster = 1")
    conn.commit()

    arena_non_booster: set[int] = set()
    setcn_non_booster: set[tuple[str, str]] = set()

    scanned = 0
    with cache_path.open("rb") as f:
        for card in ijson.items(f, "item"):
            scanned += 1
            if scanned % 500_000 == 0:
                print(f"  scanned {scanned} Scryfall cards...", file=sys.stderr, flush=True)
            if card.get("lang") != "en":
                continue
            if card.get("booster", True):
                continue
            aid = card.get("arena_id")
            if aid is not None:
                arena_non_booster.add(aid)
            set_code = (card.get("set") or "").lower()
            cn = str(card.get("collector_number") or "")
            if set_code and cn:
                setcn_non_booster.add((set_code, cn))

    conn.executemany(
        "UPDATE cards SET booster = 0 WHERE arena_id = ?",
        [(a,) for a in arena_non_booster],
    )
    conn.commit()
    by_arena = conn.execute("SELECT COUNT(*) FROM cards WHERE booster = 0").fetchone()[0]

    our_rows = conn.execute(
        "SELECT arena_id, LOWER(set_code) AS sc, collector_number AS cn "
        "FROM cards WHERE booster = 1"
    ).fetchall()
    fallback = [
        (r["arena_id"],)
        for r in our_rows
        if (r["sc"], str(r["cn"]) if r["cn"] else "") in setcn_non_booster
    ]
    conn.executemany("UPDATE cards SET booster = 0 WHERE arena_id = ?", fallback)
    conn.commit()
    total = conn.execute("SELECT COUNT(*) FROM cards WHERE booster = 0").fetchone()[0]
    conn.close()
    return by_arena, total - by_arena


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--cache",
        type=Path,
        default=DATA_DIR / "scryfall_all_cards.json",
        help="Path to the Scryfall bulk cache file.",
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=None,
        help="Path to the SQLite DB (defaults to data/mtga_collection.db).",
    )
    args = parser.parse_args()

    if not args.cache.exists():
        print(
            f"Scryfall cache not found at {args.cache}. Run Refresh Scryfall from "
            "the Settings page once to download it, or pass --cache.",
            file=sys.stderr,
        )
        return 1

    by_arena, by_setcn = backfill_booster(args.cache, args.db)
    print(f"Marked booster=0: {by_arena} by arena_id, {by_setcn} by (set, cn) fallback.")
    print(f"Total booster=0 rows: {by_arena + by_setcn}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
