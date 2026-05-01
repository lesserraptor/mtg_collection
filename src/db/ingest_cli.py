"""ingest_cli.py — End-to-end CLI entry point for the MTGA data pipeline.

Wires schema init, native CardDB ingest, Scryfall bulk download + enrichment,
and collection upsert into a single command:

    python src/db/ingest_cli.py [OPTIONS]

Phase 6 source hierarchy:
  [1/5] Database init        — init_db()
  [2/5] Native CardDB load   — find_card_db() + ingest_mtga_card_db()
  [3/5] Scryfall enrichment  — download_bulk() (if needed) + ingest_scryfall()
  [4/5] Collection upsert    — upsert_collection()
  [5/5] Errata + Summary     — apply_errata()

Arguments:
    --db PATH              Override DB path (default: ~/mtga_collection.db)
    --scryfall-cache PATH  Where to store/reuse downloaded Scryfall JSON
                           (default: ~/.cache/mtga/scryfall_all_cards.json)
    --collection PATH      Untapped collection JSON path (default: auto-detect)
    --skip-download        Skip Scryfall download; use existing cache file
    --skip-collection      Skip collection upsert step
    --skip-mtga-card-db    Skip native CardDB step (e.g. MTGA not installed)
"""

import argparse
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# When run as a script (python src/db/ingest_cli.py), the project root is not
# automatically on sys.path. Add it so `src.*` imports resolve correctly.
_HERE = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(os.path.dirname(_HERE))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from src.config import DATA_DIR
from src.db.schema import DB_PATH, init_db
from src.db.ingest import get_bulk_download_uri, download_bulk, ingest_scryfall, backfill_rebalanced_images, enrich_missing_from_api
from src.db.mtga_card_db import find_card_db, ingest_mtga_card_db
from src.collection import find_collection_file, upsert_collection
from src.db.errata import apply_errata

DEFAULT_CACHE_PATH = DATA_DIR / "scryfall_all_cards.json"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="MTGA data pipeline: native CardDB + Scryfall enrichment + collection load"
    )
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
        help=f"Scryfall cache path (default: {DEFAULT_CACHE_PATH})",
    )
    parser.add_argument(
        "--collection",
        type=Path,
        default=None,
        metavar="PATH",
        help="Untapped collection JSON path (default: auto-detect)",
    )
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Skip Scryfall download; use existing cache file",
    )
    parser.add_argument(
        "--skip-collection",
        action="store_true",
        help="Skip collection upsert step",
    )
    parser.add_argument(
        "--skip-mtga-card-db",
        action="store_true",
        help="Skip native MTGA CardDB step (use when MTGA is not installed)",
    )

    args = parser.parse_args()
    cache_path: Path = args.scryfall_cache

    # ------------------------------------------------------------------ #
    # [1/5] Database init
    # ------------------------------------------------------------------ #
    print("[1/5] Initializing database...")
    conn = init_db(args.db)
    print(f"      DB: {args.db}")

    # ------------------------------------------------------------------ #
    # [2/5] Native CardDB load
    # ------------------------------------------------------------------ #
    mtga_count = 0
    if not args.skip_mtga_card_db:
        print("[2/5] Loading native MTGA card catalog...")
        card_db_path = find_card_db()
        if card_db_path is None:
            print("ERROR: Native MTGA CardDatabase not found. Expected at:")
            print("  ~/Library/Application Support/com.wizards.mtga/Downloads/Raw/Raw_CardDatabase_*.mtga")
            print("Ensure MTGA is installed. Exiting.", file=sys.stderr)
            sys.exit(1)
        print(f"      CardDB: {card_db_path}")

        # Clear all cards before fresh ingest (both sources together form the catalog)
        conn.execute("DELETE FROM cards")
        conn.commit()

        t0 = time.time()
        mtga_count = ingest_mtga_card_db(conn, card_db_path)
        elapsed = time.time() - t0
        print(f"      Inserted {mtga_count} primary cards in {elapsed:.1f}s.")

        now = datetime.now(timezone.utc).isoformat()
        conn.execute("INSERT OR REPLACE INTO meta VALUES ('mtga_card_db_last_updated', ?)", (now,))
        conn.commit()
    else:
        print("[2/5] Skipping native MTGA CardDB (--skip-mtga-card-db).")

    # ------------------------------------------------------------------ #
    # [3/5] Scryfall enrichment
    # ------------------------------------------------------------------ #
    card_count = 0
    if not args.skip_download:
        print("[3/5] Fetching Scryfall bulk data index...")
        uri = get_bulk_download_uri()
        print("[3/5] Downloading all_cards (~2 GB)...")
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        t0 = time.time()
        download_bulk(uri, cache_path)
        elapsed = time.time() - t0
        print(f"      Downloaded in {elapsed:.1f}s → {cache_path}")
    else:
        print(f"[3/5] Using cached Scryfall data: {cache_path}")
        if not cache_path.exists():
            print(
                f"ERROR: Cache file not found: {cache_path}\n"
                "       Remove --skip-download to fetch it.",
                file=sys.stderr,
            )
            sys.exit(1)

    # Build fallback maps from the cards table so Scryfall can resolve arena_ids
    # for sets where Scryfall leaves arena_id=None (e.g. FIN, TLA, EOE, ECL).
    # Native CardDB is authoritative for these arena_ids; the maps let Scryfall
    # match by (set, collector_number) or name when arena_id is absent.
    rows = conn.execute("SELECT arena_id, name, set_code, collector_number FROM cards").fetchall()
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

    # Map (set_lower, base_collector_number) -> arena_id for rebalanced cards.
    # Scryfall stores alchemy entries as collector_number "A-{N}"; MTGA DB stores
    # them with just "{N}" and is_rebalanced=1.
    rebalanced_id_map: dict = {}
    for arena_id, set_code, cn in conn.execute(
        "SELECT arena_id, set_code, collector_number FROM cards WHERE is_rebalanced=1"
    ):
        if set_code and cn:
            rebalanced_id_map[(set_code.lower(), str(cn))] = arena_id

    print("[3/5] Enriching cards with Scryfall data...")
    t0 = time.time()
    card_count = ingest_scryfall(
        cache_path, conn,
        arena_id_map=arena_id_map,
        name_set_map=name_set_map,
        name_map=name_map,
        src_name_by_id=src_name_by_id,
        rebalanced_id_map=rebalanced_id_map,
    )
    elapsed = time.time() - t0
    print(f"      Processed {card_count} Scryfall Arena cards in {elapsed:.1f}s.")

    backfilled = backfill_rebalanced_images(conn)
    print(f"      Backfilled images for {backfilled} rebalanced cards (no Scryfall A- entry).")

    api_enriched = enrich_missing_from_api(conn)
    print(f"      API-enriched {api_enriched} cards missed by bulk ingest (set code mismatch).")

    # ------------------------------------------------------------------ #
    # [4/5] Collection upsert
    # ------------------------------------------------------------------ #
    collection_count = 0
    if not args.skip_collection:
        if args.collection:
            collection_path = args.collection
        else:
            collection_path = find_collection_file()

        if collection_path and Path(collection_path).exists():
            print(f"[4/5] Loading collection from {collection_path}...")
            t0 = time.time()
            collection_count = upsert_collection(conn, collection_path)
            elapsed = time.time() - t0
            print(f"      Loaded {collection_count} collection entries in {elapsed:.1f}s.")
        else:
            print(
                "[4/5] No collection file found. Skipping. "
                "(Run again with --collection PATH)"
            )
    else:
        print("[4/5] Skipping collection upsert (--skip-collection).")

    # ------------------------------------------------------------------ #
    # [5/5] Summary
    # ------------------------------------------------------------------ #
    # ------------------------------------------------------------------ #
    # [5/5] Errata + Summary
    # ------------------------------------------------------------------ #
    errata_count = apply_errata(conn)

    print()
    print("=== Ingest complete ===")
    print(f"  MTGA native cards:  {mtga_count}")
    print(f"  Scryfall enriched:  {card_count}")
    print(f"  Collection rows:    {collection_count}")
    print(f"  Errata applied:     {errata_count}")
    print(f"  DB path:            {args.db}")
    conn.close()


if __name__ == "__main__":
    main()
