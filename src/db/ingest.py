"""Scryfall bulk data download and streaming ingest module.

Provides get_bulk_download_uri(), download_bulk(), ingest_scryfall(),
_extract_image_uris(), and _card_to_rows() for the MTGA collection browser.
"""

import json
import sqlite3
import sys
from pathlib import Path

import ijson
import requests

SCRYFALL_BULK_INDEX = "https://api.scryfall.com/bulk-data"
SCRYFALL_BULK_TYPE = "all_cards"
BATCH_SIZE = 500
USER_AGENT = "mtga-browser/1.0"

# Enrichment UPDATE: fills Scryfall-sourced fields only when native CardDB
# left them NULL/empty. Native CardDB values are never overwritten.
# Fields overwritten unconditionally:
# - booster: Scryfall is authoritative; native CardDB has no equivalent flag.
# - oracle_text: multi-face cards (split/adventure/flip/prepare) sometimes enter
#   with only the front face's text when ingest logic changes, and errata runs
#   after enrichment so user corrections still win.
ENRICH_CARD_SQL = """
    UPDATE cards SET
        scryfall_id      = ?,
        mana_cost        = ?,
        cmc              = COALESCE(cmc, ?),
        type_line        = COALESCE(type_line, ?),
        oracle_text      = ?,
        rarity           = COALESCE(NULLIF(rarity,'common'), ?),
        set_code         = COALESCE(NULLIF(set_code,''), ?),
        collector_number = COALESCE(collector_number, ?),
        colors           = COALESCE(NULLIF(colors,'[]'), ?),
        color_identity   = COALESCE(NULLIF(color_identity,'[]'), ?),
        keywords         = COALESCE(NULLIF(keywords,'[]'), ?),
        layout           = COALESCE(layout, ?),
        image_uri_front  = ?,
        image_uri_back   = ?,
        booster          = ?
    WHERE arena_id = ?
"""

# Insert for cards Scryfall knows that native CardDB does not have.
# INSERT OR IGNORE ensures existing native rows are never replaced.
INSERT_NEW_CARD_SQL = """
    INSERT OR IGNORE INTO cards (
        arena_id, scryfall_id, name, mana_cost, cmc, type_line, oracle_text,
        rarity, set_code, collector_number, colors, color_identity, keywords,
        layout, image_uri_front, image_uri_back, is_rebalanced, booster
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
"""


def get_bulk_download_uri(bulk_type: str = SCRYFALL_BULK_TYPE) -> str:
    """Fetch the download URI for the given bulk data type from Scryfall index.

    Args:
        bulk_type: Scryfall bulk data type name. Defaults to "default_cards".

    Returns:
        The download_uri string for the matching bulk data entry.

    Raises:
        ValueError: If no entry matches the given bulk_type.
        requests.HTTPError: If the Scryfall index request fails.
    """
    resp = requests.get(
        SCRYFALL_BULK_INDEX,
        timeout=30,
        headers={"User-Agent": USER_AGENT},
    )
    resp.raise_for_status()
    data = resp.json()
    for entry in data.get("data", []):
        if entry.get("type") == bulk_type:
            return entry["download_uri"]
    raise ValueError(
        f"No Scryfall bulk data entry found with type={bulk_type!r}"
    )


def download_bulk(uri: str, dest_path: Path, progress: bool = True, progress_callback=None) -> None:
    """Stream-download a Scryfall bulk data file to dest_path.

    Never loads the full file into memory. Prints a dot to stderr every 10 MB
    when progress=True so stdout is not polluted.

    Args:
        uri: Direct download URI for the bulk data file.
        dest_path: Destination path. Parent directories are created as needed.
        progress: If True, print dots to stderr every 10 MB.
        progress_callback: Optional callable(stage, rows, total, message). Called
            at download start and completion to report progress.
    """
    dest_path = Path(dest_path)
    dest_path.parent.mkdir(parents=True, exist_ok=True)

    if progress_callback:
        progress_callback("download", 0, 0, "Downloading bulk data from Scryfall...")

    resp = requests.get(
        uri,
        stream=True,
        timeout=120,
        headers={"User-Agent": USER_AGENT},
    )
    resp.raise_for_status()

    bytes_written = 0
    dot_threshold = 10 * 1024 * 1024  # 10 MB
    next_dot = dot_threshold

    with dest_path.open("wb") as f:
        for chunk in resp.iter_content(chunk_size=65536):
            if chunk:
                f.write(chunk)
                bytes_written += len(chunk)
                if progress and bytes_written >= next_dot:
                    print(".", end="", flush=True, file=sys.stderr)
                    next_dot += dot_threshold

    if progress:
        print(file=sys.stderr)  # newline after dots

    if progress_callback:
        progress_callback("download", 0, 0, "Download complete, writing cache...")


def _extract_image_uris(card: dict) -> dict:
    """Extract front and back image URIs from a Scryfall card object.

    Order matters — top-level image_uris is checked first because adventure
    cards have both top-level image_uris AND card_faces, and the top-level
    image represents the correct single image for such cards.

    Args:
        card: A Scryfall card object as a dict.

    Returns:
        Dict with "front" (str | None) and "back" (str | None) keys.
    """
    if "image_uris" in card:
        # Normal cards and adventure cards — top-level image_uris wins
        return {
            "front": card["image_uris"].get("normal"),
            "back": None,
        }
    elif card.get("card_faces"):
        # Transform, modal_dfc, split, flip — each face has its own image_uris
        faces = card["card_faces"]
        front = None
        back = None
        if len(faces) > 0 and faces[0].get("image_uris"):
            front = faces[0]["image_uris"].get("normal")
        if len(faces) > 1 and faces[1].get("image_uris"):
            back = faces[1]["image_uris"].get("normal")
        return {"front": front, "back": back}
    else:
        return {"front": None, "back": None}


# Layouts where both halves are physically visible on one face (split/room, adventure,
# Kamigawa flip, and Scryfall's `prepare` variant used for adventure-like reprints in
# SLD/prerelease/plst). For these, Scryfall leaves the top-level oracle_text null and
# splits the text across card_faces — we have to join it or half the card goes missing.
_BOTH_HALVES_VISIBLE_LAYOUTS = frozenset({"split", "adventure", "flip", "prepare"})


def _extract_oracle_text(card: dict) -> str | None:
    """Return the oracle text to store for a Scryfall card, handling multi-face layouts."""
    top = card.get("oracle_text")
    if top:
        return top
    faces = card.get("card_faces") or []
    if not faces:
        return None
    if card.get("layout") in _BOTH_HALVES_VISIBLE_LAYOUTS:
        parts = [f.get("oracle_text", "") for f in faces if f.get("oracle_text")]
        return "\n//\n".join(parts) if parts else None
    # DFC / transform / meld / etc.: the front face is the "main" side; the back lives
    # on its own physical face and is shown separately by the card-detail UI.
    return faces[0].get("oracle_text")


def _card_to_rows(card: dict) -> tuple[tuple, tuple]:
    """Convert a Scryfall card dict to two tuples for enrichment and insert.

    Returns:
        (enrich_tuple, insert_tuple) where:
        - enrich_tuple matches ENRICH_CARD_SQL params:
          scryfall_id, mana_cost, cmc, type_line, oracle_text, rarity,
          set_code, collector_number, colors, color_identity, keywords,
          layout, image_uri_front, image_uri_back, booster, arena_id
        - insert_tuple matches INSERT_NEW_CARD_SQL params (18 elements):
          arena_id, scryfall_id, name, mana_cost, cmc, type_line,
          oracle_text, rarity, set_code, collector_number, colors,
          color_identity, keywords, layout, image_uri_front,
          image_uri_back, is_rebalanced, booster

    Args:
        card: A Scryfall card dict. Caller must ensure arena_id is non-null.
    """
    images = _extract_image_uris(card)
    arena_id = card["arena_id"]
    scryfall_id = card.get("id")
    name = card.get("name")
    # For DFCs (transform, modal_dfc, etc.), mana_cost is None at the top level
    # and lives in card_faces[0] for the front face. Oracle text needs layout-aware
    # handling: split/adventure/flip cards expose both halves simultaneously and
    # Scryfall keeps their text split across card_faces.
    front_face = (card.get("card_faces") or [{}])[0]
    mana_cost = card.get("mana_cost") or front_face.get("mana_cost")
    cmc = float(card["cmc"]) if card.get("cmc") is not None else None
    type_line = card.get("type_line")
    oracle_text = _extract_oracle_text(card)
    rarity = card.get("rarity")
    set_code = card.get("set")           # Scryfall field is "set", not "set_code"
    collector_number = card.get("collector_number")
    # For DFCs, top-level colors is [] — actual colors live in card_faces[0].
    colors = json.dumps(card.get("colors") or front_face.get("colors", []))
    color_identity = json.dumps(card.get("color_identity", []))
    keywords = json.dumps(card.get("keywords", []))
    layout = card.get("layout")
    image_front = images["front"]
    image_back = images["back"]
    booster = 1 if card.get("booster", True) else 0

    enrich_tuple = (
        scryfall_id,
        mana_cost,
        cmc,
        type_line,
        oracle_text,
        rarity,
        set_code,
        collector_number,
        colors,
        color_identity,
        keywords,
        layout,
        image_front,
        image_back,
        booster,
        arena_id,  # WHERE clause — must be last
    )

    insert_tuple = (
        arena_id,
        scryfall_id,
        name,
        mana_cost,
        cmc,
        type_line,
        oracle_text,
        rarity,
        set_code,
        collector_number,
        colors,
        color_identity,
        keywords,
        layout,
        image_front,
        image_back,
        0,           # is_rebalanced: CardDatabase is authoritative
        booster,
    )

    return enrich_tuple, insert_tuple


_BASIC_LAND_NAMES = {"Plains", "Island", "Swamp", "Mountain", "Forest", "Wastes"}


def _names_match(scryfall_name: str, src_name: str) -> bool:
    """Return True if scryfall_name and src_name refer to the same card.

    Handles DFC suffix ("Front // Back" vs "Front"), unicode normalization
    differences, and minor punctuation variants.
    """
    import unicodedata
    def norm(s: str) -> str:
        return unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode().lower()
    sn = norm(scryfall_name)
    srn = norm(src_name)
    return sn == srn or sn.startswith(srn + " //")


def _set_cn_match_ok(scryfall_name: str, src_name: str) -> bool:
    """Return True if a (set, collector_number) fallback match is acceptable.

    Allows flavor-name mismatches (e.g. Spider-Man physical vs Arena names)
    but rejects matches where either card is a basic land with a different
    name — those indicate collector number collisions between paper and Arena.
    """
    if _names_match(scryfall_name, src_name):
        return True
    if scryfall_name in _BASIC_LAND_NAMES or src_name in _BASIC_LAND_NAMES:
        return False
    return True  # flavor name difference — accept


def ingest_scryfall(
    json_path: Path,
    conn: sqlite3.Connection,
    batch_size: int = BATCH_SIZE,
    arena_id_map: dict | None = None,
    name_set_map: dict | None = None,
    name_map: dict | None = None,
    src_name_by_id: dict | None = None,
    rebalanced_id_map: dict | None = None,
    progress_callback=None,
) -> int:
    """Stream-parse a Scryfall bulk JSON file and enrich Arena cards in SQLite.

    Opens the file in binary mode (required by ijson). Skips cards without
    arena_id unless fallback maps resolve it:
      1. arena_id_map: (set_lower, collector_number) -> arena_id
      2. name_set_map: (card_name, set_lower) -> arena_id
      3. name_map: card_name -> arena_id (only unambiguous entries)

    Skips rebalanced variants (collector_number starting with "A-") because
    native CardDB is authoritative and A- entries cause duplicate art issues
    for the 66 arena_ids that have both A- and non-A- Scryfall entries.

    For each resolved Arena card:
    - If the card already exists (ingested from native CardDB): UPDATE to fill
      image_uri, oracle_text, cmc, layout, type_line — native values win via
      COALESCE (no overwrite of non-null native data).
    - If the card does not exist in native CardDB: INSERT OR IGNORE as fallback.

    Processes in batches of batch_size. Prints progress to stderr every 5000
    cards.

    Args:
        json_path: Path to the Scryfall bulk JSON file.
        conn: An open sqlite3.Connection with the cards table present.
        batch_size: Number of rows per executemany() call.
        arena_id_map: Dict mapping (set_lower, collector_number) to arena_id.
        name_set_map: Dict mapping (card_name, set_lower) to arena_id.
        name_map: Dict mapping card name to arena_id for unambiguous names only.
        src_name_by_id: Dict mapping arena_id to source DB card name, used to
            reject fallback matches where set codes collide across different sets.

    Returns:
        Total number of rows processed (enriched or inserted).
    """
    json_path = Path(json_path)
    arena_id_map = arena_id_map or {}
    name_set_map = name_set_map or {}
    name_map = name_map or {}
    src_name_by_id = src_name_by_id or {}
    rebalanced_id_map = rebalanced_id_map or {}
    enrich_batch = []
    insert_batch = []
    total = 0

    # Tier 4 map: name_lower -> [arena_ids still lacking scryfall_id].
    # Used to match Arena cards whose Scryfall set code differs from the MTGA
    # CardDB set code (e.g. y25 vs ytdm, yecl, etc.) — these slip through tiers
    # 1-3 because all three tiers rely on matching set codes or unambiguous names.
    # Tier 4 accepts any Arena card whose name matches exactly one unclaimed DB
    # entry that still has no scryfall_id, including cards with multiple printings
    # and special-frame collaboration cards (Final Fantasy etc.).
    _t4: dict[str, list[int]] = {}
    for _aid, _name in conn.execute(
        "SELECT arena_id, name FROM cards WHERE (scryfall_id IS NULL OR scryfall_id = '') AND is_rebalanced = 0"
    ):
        if _name:
            _t4.setdefault(_name.lower(), []).append(_aid)
    _t4_claimed: set[int] = set()

    with json_path.open("rb") as f:
        for card in ijson.items(f, "item"):
            if card.get("lang") != "en":
                continue
            arena_id = card.get("arena_id")
            # Guard: Scryfall sometimes maps a basic land arena_id to the wrong
            # land type (e.g. LCI CN=400 arena_id=87461 is a Mountain in Scryfall
            # but a Forest in MTGA). Reject direct-match enrichment when both the
            # Scryfall name and the DB name are basics but they differ.
            if arena_id is not None and src_name_by_id:
                scryfall_name = card.get("name", "")
                db_name = src_name_by_id.get(arena_id)
                if (db_name and scryfall_name != db_name
                        and scryfall_name in _BASIC_LAND_NAMES
                        and db_name in _BASIC_LAND_NAMES):
                    continue
            if arena_id is None:
                if "arena" not in card.get("games", []):
                    continue
                name = card.get("name", "")
                set_lower = card.get("set", "").lower()
                cn = card.get("collector_number", "")
                use_src_name = False

                # Tier 1: (set, cn) match — reject basic land collisions,
                # allow flavor-name differences (Spider-Man sets etc.)
                arena_id = arena_id_map.get((set_lower, cn))
                if arena_id is not None:
                    src_name = src_name_by_id.get(arena_id)
                    if src_name and not _set_cn_match_ok(name, src_name):
                        arena_id = None
                    else:
                        use_src_name = True  # prefer Arena name from source DB

                if arena_id is None:
                    # Tiers 2 & 3: name-based — require names to match.
                    # Skip non-standard variants (borderless, showcase, etc.) at
                    # this tier — they share names with normal cards but have
                    # different collector numbers and wrong art for the base card.
                    if card.get("border_color") == "borderless" or card.get("frame_effects"):
                        arena_id = None
                    else:
                        arena_id = name_set_map.get((name, set_lower)) or name_map.get(name)
                        if arena_id is not None:
                            src_name = src_name_by_id.get(arena_id)
                            if src_name and not _names_match(name, src_name):
                                arena_id = None

                # Tier 4: name-only match against DB cards still lacking scryfall_id.
                # Handles set-code mismatches (Alchemy year codes, collaboration
                # frames, SPG reprints) that all prior tiers miss. Only fires when
                # exactly one unclaimed DB candidate exists for this name.
                if arena_id is None:
                    name_lower = name.lower()
                    candidates = [
                        aid for aid in _t4.get(name_lower, [])
                        if aid not in _t4_claimed
                    ]
                    if len(candidates) == 1:
                        src_name = src_name_by_id.get(candidates[0])
                        if not src_name or _names_match(name, src_name):
                            arena_id = candidates[0]
                            _t4_claimed.add(arena_id)

                if arena_id is None:
                    continue

                override = {"arena_id": arena_id}
                if use_src_name and arena_id in src_name_by_id:
                    override["name"] = src_name_by_id[arena_id]
                card = {**card, **override}
                card = {**card, "arena_id": arena_id}

            # Handle A- (alchemy-rebalanced) Scryfall entries separately.
            # These have distinct art and oracle text but no arena_id in Scryfall's data.
            # Match via rebalanced_id_map: (set_lower, base_collector_number) -> arena_id.
            cn = card.get("collector_number", "")
            if cn.startswith("A-"):
                set_lower = card.get("set", "").lower()
                base_cn = cn[2:]
                reb_arena_id = rebalanced_id_map.get((set_lower, base_cn))
                if reb_arena_id:
                    card = {**card, "arena_id": reb_arena_id}
                else:
                    continue

            enrich_row, insert_row = _card_to_rows(card)
            enrich_batch.append(enrich_row)
            insert_batch.append(insert_row)

            if len(enrich_batch) >= batch_size:
                conn.executemany(ENRICH_CARD_SQL, enrich_batch)
                conn.executemany(INSERT_NEW_CARD_SQL, insert_batch)
                conn.commit()
                total += len(enrich_batch)
                enrich_batch = []
                insert_batch = []
                if total % 5000 == 0:
                    print(f"Processed {total} cards...", file=sys.stderr)
                if progress_callback:
                    # Estimate pct: Arena cards ~95k; clamp to 99 until done
                    pct = min(20 + int(total / 95000 * 79), 99)
                    progress_callback("enrich", total, 95000, f"Enriching card {total}...")

        # Flush remaining batch
        if enrich_batch:
            conn.executemany(ENRICH_CARD_SQL, enrich_batch)
            conn.executemany(INSERT_NEW_CARD_SQL, insert_batch)
            conn.commit()
            total += len(enrich_batch)

    return total


def enrich_missing_from_api(
    conn: sqlite3.Connection,
    progress_callback=None,
) -> int:
    """Enrich cards that bulk ingest missed by querying Scryfall's named endpoint.

    The bulk ingest can only match cards whose Scryfall set code matches the
    MTGA CardDB set code. Some sets (e.g. y25 vs ytdm, spg, fin) diverge,
    leaving cards with mana_cost populated (from native CardDB) but no
    scryfall_id, cmc, oracle_text, type_line, keywords, layout, or image URIs.

    This pass finds all such cards and fetches their data from the Scryfall API
    one card at a time. Respects Scryfall's rate limit (100ms between requests).

    Returns the number of cards successfully enriched.
    """
    import time

    missing = conn.execute("""
        SELECT arena_id, name FROM cards
        WHERE cmc IS NULL
          AND is_rebalanced = 0
          AND scryfall_id IS NOT NULL AND scryfall_id != ''
        ORDER BY arena_id
    """).fetchall()

    if not missing:
        return 0

    enriched = 0
    for i, (arena_id, name) in enumerate(missing):
        if progress_callback:
            progress_callback("api_enrich", i, len(missing),
                              f"Fetching {name} from Scryfall API...")
        try:
            resp = requests.get(
                f"https://api.scryfall.com/cards/named?exact={requests.utils.quote(name)}",
                headers={"User-Agent": USER_AGENT},
                timeout=10,
            )
            if resp.status_code != 200:
                continue
            card = resp.json()
            images = _extract_image_uris(card)
            front_face = (card.get("card_faces") or [{}])[0]
            conn.execute(ENRICH_CARD_SQL, (
                card.get("id"),
                card.get("mana_cost") or front_face.get("mana_cost"),
                float(card["cmc"]) if card.get("cmc") is not None else None,
                card.get("type_line"),
                _extract_oracle_text(card),
                card.get("rarity"),
                card.get("set"),
                card.get("collector_number"),
                json.dumps(card.get("colors") or front_face.get("colors", [])),
                json.dumps(card.get("color_identity", [])),
                json.dumps(card.get("keywords", [])),
                card.get("layout"),
                images["front"],
                images["back"],
                1 if card.get("booster", True) else 0,
                arena_id,
            ))
            conn.commit()
            enriched += 1
        except Exception:
            pass
        time.sleep(0.1)

    return enriched


def backfill_rebalanced_images(conn) -> int:
    """Copy scryfall_id and image URIs from standard cards to their rebalanced variants.

    Rebalanced (is_rebalanced=1) cards use the same physical card art as their
    standard counterpart — they share set_code and collector_number. Scryfall has
    no separate entries for MTGA alchemy rewrites, so the enrichment step skips
    them. This pass fills the gap by copying image data from the matched standard row.

    Returns the number of rows updated.
    """
    cursor = conn.execute("""
        UPDATE cards SET
            scryfall_id = (
                SELECT std.scryfall_id FROM cards std
                WHERE std.set_code = cards.set_code
                  AND std.collector_number = cards.collector_number
                  AND std.is_rebalanced = 0
                  AND std.scryfall_id != ''
                LIMIT 1
            ),
            image_uri_front = (
                SELECT std.image_uri_front FROM cards std
                WHERE std.set_code = cards.set_code
                  AND std.collector_number = cards.collector_number
                  AND std.is_rebalanced = 0
                  AND std.scryfall_id != ''
                LIMIT 1
            ),
            image_uri_back = (
                SELECT std.image_uri_back FROM cards std
                WHERE std.set_code = cards.set_code
                  AND std.collector_number = cards.collector_number
                  AND std.is_rebalanced = 0
                  AND std.scryfall_id != ''
                LIMIT 1
            )
        WHERE is_rebalanced = 1 AND (scryfall_id = '' OR scryfall_id IS NULL)
    """)
    conn.commit()
    return cursor.rowcount
