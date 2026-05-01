"""Backfill oracle_text for multi-face cards from the cached Scryfall bulk data.

Use this when a DB was enriched before the split/adventure/flip/prepare layout
fix landed: previously the ingest only captured the first face's oracle text,
so split cards (Rooms etc.) showed only one half of their rules. This script
streams data/scryfall_all_cards.json and rewrites oracle_text for matching
layouts using _extract_oracle_text, which joins the two halves with "\\n//\\n".

Safe to re-run — it only touches rows whose layout is one of the two-halves-
visible set, matched by arena_id (with a (set, collector_number) fallback).

Usage:
    python -m scripts.backfill_oracle [--cache PATH] [--db PATH]
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import ijson

from src.config import DATA_DIR
from src.db.ingest import _BOTH_HALVES_VISIBLE_LAYOUTS, _extract_oracle_text
from src.db.schema import init_db


def backfill_oracle(cache_path: Path, db_path: Path | None = None) -> tuple[int, int]:
    """Return (updates_by_arena_id, updates_by_set_cn_fallback)."""
    conn = init_db(db_path)

    fixed_aid: dict[int, str] = {}
    fixed_setcn: dict[tuple[str, str], str] = {}
    scanned = 0
    with cache_path.open("rb") as f:
        for card in ijson.items(f, "item"):
            scanned += 1
            if scanned % 500_000 == 0:
                print(f"  scanned {scanned} Scryfall cards...", file=sys.stderr, flush=True)
            if card.get("lang") != "en":
                continue
            if card.get("layout") not in _BOTH_HALVES_VISIBLE_LAYOUTS:
                continue
            text = _extract_oracle_text(card)
            if not text:
                continue
            aid = card.get("arena_id")
            if aid is not None:
                fixed_aid[aid] = text
            set_code = (card.get("set") or "").lower()
            cn = str(card.get("collector_number") or "")
            if set_code and cn:
                fixed_setcn[(set_code, cn)] = text

    by_arena = conn.executemany(
        "UPDATE cards SET oracle_text = ? WHERE arena_id = ?",
        [(v, k) for k, v in fixed_aid.items()],
    ).rowcount or 0
    conn.commit()

    remaining = conn.execute(
        "SELECT arena_id, LOWER(set_code) AS sc, collector_number AS cn, oracle_text "
        "FROM cards WHERE layout IN (" + ",".join("?" * len(_BOTH_HALVES_VISIBLE_LAYOUTS)) + ") "
        "  AND (oracle_text NOT LIKE '%//%' OR oracle_text IS NULL)",
        tuple(_BOTH_HALVES_VISIBLE_LAYOUTS),
    ).fetchall()
    fallback_updates = []
    for r in remaining:
        key = (r["sc"], str(r["cn"]) if r["cn"] else "")
        new = fixed_setcn.get(key)
        if new and new != r["oracle_text"]:
            fallback_updates.append((new, r["arena_id"]))
    by_setcn = conn.executemany(
        "UPDATE cards SET oracle_text = ? WHERE arena_id = ?",
        fallback_updates,
    ).rowcount or 0
    conn.commit()
    conn.close()
    return by_arena, by_setcn


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

    by_arena, by_setcn = backfill_oracle(args.cache, args.db)
    print(f"oracle_text updates: {by_arena} by arena_id, {by_setcn} by (set, cn) fallback.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
