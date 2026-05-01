#!/usr/bin/env python3
"""
mtga_collection.py - Build an MTGA card collection SQLite database.

Primary source of card quantities is the Untapped collection JSON
(captured from Untapped.gg companion via patched asar). Falls back to
deck-based counting if no collection file is available.

Usage:
    python mtga_collection.py [--collection PATH] [--log PATH] [--db PATH]
                              [--card-db PATH] [--scryfall-cache PATH]
                              [--no-images]
"""

import argparse
import glob
import json
import os
import re
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

DEFAULT_LOG_PATHS = [
    # Linux / Steam (Proton)
    Path.home() / ".local/share/Steam/steamapps/compatdata/2141910/pfx/drive_c/users/steamuser/AppData/LocalLow/Wizards Of The Coast/MTGA/Player.log",
    # macOS
    Path.home() / "Library/Logs/Wizards Of The Coast/MTGA/Player.log",
    # Windows
    Path(os.environ.get("LOCALAPPDATA", "~")).expanduser().parent / "LocalLow/Wizards Of The Coast/MTGA/Player.log",
]

CARD_DB_GLOBS = [
    # Linux / Steam
    str(Path.home() / ".local/share/Steam/steamapps/common/MTGA/MTGA_Data/Downloads/Raw/Raw_CardDatabase_*.mtga"),
    # macOS
    str(Path.home() / "Library/Application Support/com.wizards.mtga/Downloads/Raw/Raw_CardDatabase_*.mtga"),
    # Windows
    str(Path(os.environ.get("PROGRAMFILES", "C:/Program Files")) / "Wizards of the Coast/MTGA/MTGA_Data/Downloads/Raw/Raw_CardDatabase_*.mtga"),
]

DEFAULT_COLLECTION_PATHS = [
    # Written by patched Untapped.gg asar
    Path.home() / ".local/share/Steam/steamapps/compatdata/2141910/pfx/drive_c/x.json",
]

SCRYFALL_CACHE_PATH = Path.home() / ".cache/mtga/scryfall_default_cards.json"
SCRYFALL_CACHE_MAX_AGE = 86400  # 24h
SCRYFALL_BULK_INDEX = "https://api.scryfall.com/bulk-data"

CARD_SECTIONS = ("MainDeck", "Sideboard", "CommandZone", "Companions")

RARITY_MAP = {0: "token", 1: "basic", 2: "common", 3: "uncommon", 4: "rare", 5: "mythic"}

SOURCE_COLLECTION = "collection"
SOURCE_GRANTED    = "granted"


# ---------------------------------------------------------------------------
# Untapped collection JSON
# ---------------------------------------------------------------------------

def find_collection() -> Path | None:
    for p in DEFAULT_COLLECTION_PATHS:
        if p.exists():
            return p
    return None


def parse_untapped_collection(path: Path) -> dict[int, int]:
    """Return {arena_id: quantity} from Untapped collection JSON."""
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    return {int(c["grpid"]): int(c["quantity"]) for c in data["cards"]}


# ---------------------------------------------------------------------------
# Log parsing
# ---------------------------------------------------------------------------

def find_log() -> Path | None:
    for p in DEFAULT_LOG_PATHS:
        if p.exists():
            return p
    return None


def parse_log(log_path: Path) -> dict | None:
    """Return the most recent StartHook payload from the log."""
    data = None
    with open(log_path, encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if line.startswith("{") and '"Decks"' in line and '"InventoryInfo"' in line:
                try:
                    parsed = json.loads(line)
                    if "Decks" in parsed:
                        data = parsed
                except json.JSONDecodeError:
                    pass
    return data


def extract_deck_membership(data: dict) -> dict[int, list[str]]:
    """Return {arena_id: [deck_names]} from user-created decks in log data."""
    decks = data.get("Decks", {})
    name_by_id = {d["DeckId"]: d["Name"] for d in data.get("DeckSummariesV2", [])}

    membership: dict[int, set] = {}
    for deck_id, deck in decks.items():
        deck_name = name_by_id.get(deck_id, deck_id)
        if deck_name.startswith("?=?Loc/"):
            continue
        for section in CARD_SECTIONS:
            for entry in deck.get(section, []):
                cid = entry["cardId"]
                membership.setdefault(cid, set()).add(deck_name)

    return {cid: sorted(names) for cid, names in membership.items()}


# ---------------------------------------------------------------------------
# GrantedCards parsing
# ---------------------------------------------------------------------------

def _parse_granted_cards(changes: list) -> list[tuple[int, str, int]]:
    """
    Extract (arena_id, source_id, quantity) tuples from InventoryInfo.Changes entries.

    GrantedCards is a list of either:
      - integers (grpIds), duplicates = multiple copies
      - dicts with {"cardId": int, "quantity": int}
    """
    results = []
    for change in changes:
        source_id = change.get("SourceId", "")
        if not source_id:
            continue
        granted = change.get("GrantedCards", [])
        if not granted:
            continue

        counts: dict[int, int] = {}
        for entry in granted:
            if isinstance(entry, int):
                counts[entry] = counts.get(entry, 0) + 1
            elif isinstance(entry, dict):
                cid = entry.get("cardId") or entry.get("grpId")
                qty = entry.get("quantity", 1)
                if cid:
                    counts[int(cid)] = counts.get(int(cid), 0) + qty

        for arena_id, qty in counts.items():
            results.append((arena_id, source_id, qty))

    return results


def parse_grants_from_log(log_path: Path) -> list[tuple[int, str, int]]:
    """Scan a log file for all InventoryInfo.Changes entries with GrantedCards."""
    results = []
    with open(log_path, encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if '"InventoryInfo"' not in line and '"Changes"' not in line:
                continue
            # Changes can appear in two places:
            # 1. Top-level {"InventoryInfo": {"Changes": [...]}}  (StartHook)
            # 2. Top-level {"Course": {...}} where InventoryInfo is nested
            if not line.startswith("{"):
                continue
            try:
                data = json.loads(line)
            except json.JSONDecodeError:
                continue

            # Walk the parsed object looking for Changes lists
            def find_changes(obj):
                if isinstance(obj, dict):
                    changes = obj.get("Changes")
                    if isinstance(changes, list) and changes:
                        results.extend(_parse_granted_cards(changes))
                    for v in obj.values():
                        find_changes(v)
                elif isinstance(obj, list):
                    for item in obj:
                        find_changes(item)

            find_changes(data)

    return results


# ---------------------------------------------------------------------------
# CardDatabase
# ---------------------------------------------------------------------------

def find_card_db() -> Path | None:
    for pattern in CARD_DB_GLOBS:
        matches = glob.glob(pattern)
        if matches:
            return Path(matches[0])
    return None


def _decode_mana(raw: str) -> str:
    """Convert 'o2oWoU' → '{2}{W}{U}'."""
    if not raw:
        return ""
    return re.sub(r"o([^o]+)", r"{\1}", raw)


def _decode_ability_mana(text: str) -> str:
    """Convert mana symbol encodings inside ability oracle text."""
    # Braced form {o4oRoW} → {4}{R}{W}: decode the inner sequence as a full mana cost
    text = re.sub(r"\{o([^}]+)\}", lambda m: _decode_mana("o" + m.group(1)), text)
    # Replace CARDNAME placeholder with ~
    text = text.replace("CARDNAME", "~")
    return text


def build_card_lookup(card_db_path: Path) -> dict[int, dict]:
    """
    Build {grpId: card_info} from MTGA's local CardDatabase.
    Resolves names, type lines, oracle text, mana costs, and enums locally.
    """
    conn = sqlite3.connect(str(card_db_path))
    conn.row_factory = sqlite3.Row

    # -- Localizations --
    print("  Loading localizations…")
    loc = {row[0]: row[1] for row in conn.execute("SELECT LocId, Loc FROM Localizations_enUS")}

    # -- Enum lookups --
    color_map: dict[int, str] = {}
    type_map: dict[int, str] = {}
    subtype_map: dict[int, str] = {}
    for row in conn.execute("SELECT Type, Value, LocId FROM Enums"):
        text = loc.get(row["LocId"], "")
        if row["Type"] == "Color":
            # Map to single-letter MTG color codes
            color_map[row["Value"]] = {"White": "W", "Blue": "U", "Black": "B",
                                        "Red": "R", "Green": "G"}.get(text, text)
        elif row["Type"] == "CardType":
            type_map[row["Value"]] = text
        elif row["Type"] == "SubType":
            subtype_map[row["Value"]] = text

    def decode_list(raw: str, mapping: dict) -> list[str]:
        if not raw:
            return []
        return [mapping.get(int(v), str(v)) for v in raw.split(",") if v]

    # -- Abilities --
    print("  Loading abilities…")
    ability_text: dict[int, str] = {}
    for row in conn.execute("SELECT Id, TextId FROM Abilities"):
        text = loc.get(row["TextId"], "")
        if text:
            ability_text[row["Id"]] = _decode_ability_mana(text)

    def get_oracle_text(ability_ids_raw: str) -> str:
        if not ability_ids_raw:
            return ""
        texts = []
        for part in ability_ids_raw.split(","):
            aid_str = part.split(":")[0]
            try:
                text = ability_text.get(int(aid_str), "")
                if text:
                    texts.append(text)
            except ValueError:
                pass
        return "\n".join(texts)

    # -- Cards --
    print("  Loading cards…")
    lookup: dict[int, dict] = {}
    for row in conn.execute("SELECT * FROM Cards"):
        grp_id = row["GrpId"]

        name = loc.get(row["TitleId"], "")
        type_line = loc.get(row["TypeTextId"], "")
        subtype = loc.get(row["SubtypeTextId"], "")
        if subtype:
            type_line = f"{type_line} — {subtype}"

        colors = decode_list(row["Colors"], color_map)
        color_identity = decode_list(row["ColorIdentity"], color_map)

        lookup[grp_id] = {
            "name": name or None,
            "set_code": row["ExpansionCode"] or None,
            "collector_number": row["CollectorNumber"] or None,
            "rarity": RARITY_MAP.get(row["Rarity"], str(row["Rarity"])),
            "mana_cost": _decode_mana(row["OldSchoolManaText"]) or None,
            "type_line": type_line or None,
            "oracle_text": get_oracle_text(row["AbilityIds"]) or None,
            "colors": json.dumps(colors),
            "color_identity": json.dumps(color_identity),
            "power": row["Power"] or None,
            "toughness": row["Toughness"] or None,
            "is_token": bool(row["IsToken"]),
            "is_rebalanced": bool(row["IsRebalanced"]),
        }

    conn.close()
    print(f"  Loaded {len(lookup):,} cards from CardDatabase")
    return lookup


# ---------------------------------------------------------------------------
# Scryfall (images only)
# ---------------------------------------------------------------------------

def _fetch_url(url: str) -> bytes:
    try:
        import requests
        resp = requests.get(url, timeout=60, headers={"User-Agent": "mtga-collection-script/1.0"})
        resp.raise_for_status()
        return resp.content
    except ImportError:
        import urllib.request
        req = urllib.request.Request(url, headers={"User-Agent": "mtga-collection-script/1.0"})
        with urllib.request.urlopen(req, timeout=60) as r:
            return r.read()


def load_scryfall_images(cache_path: Path) -> dict[int, str]:
    """Return {arena_id: image_uri} from cached or downloaded Scryfall data."""
    cache_path = cache_path.expanduser()

    if cache_path.exists():
        age = time.time() - cache_path.stat().st_mtime
        if age < SCRYFALL_CACHE_MAX_AGE:
            print(f"  Using cached Scryfall data ({int(age / 3600)}h old)")
        else:
            cache_path = None  # force re-download below

    if not (cache_path and cache_path.exists()):
        print("  Fetching Scryfall bulk-data index…")
        index = json.loads(_fetch_url(SCRYFALL_BULK_INDEX))
        uri = next(e["download_uri"] for e in index["data"] if e["type"] == "default_cards")
        mb = next(e.get("size", 0) for e in index["data"] if e["type"] == "default_cards") // 1_048_576
        print(f"  Downloading Scryfall default cards (~{mb} MB)…")
        raw = _fetch_url(uri)
        cache_path = SCRYFALL_CACHE_PATH.expanduser()
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_bytes(raw)
        print(f"  Cached to {cache_path}")

    print("  Building image lookup…")
    images: dict[int, str] = {}
    with open(cache_path, encoding="utf-8") as f:
        for card in json.load(f):
            aid = card.get("arena_id")
            if aid is None:
                continue
            uri = None
            if "image_uris" in card:
                uri = card["image_uris"].get("normal")
            elif card.get("card_faces"):
                uri = card["card_faces"][0].get("image_uris", {}).get("normal")
            if uri:
                images[int(aid)] = uri
    print(f"  {len(images):,} image URIs loaded")
    return images


# ---------------------------------------------------------------------------
# SQLite
# ---------------------------------------------------------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS cards (
    arena_id         INTEGER PRIMARY KEY,
    quantity         INTEGER NOT NULL,
    source           TEXT,    -- 'deck', 'granted', or 'deck+granted'
    name             TEXT,
    set_code         TEXT,
    collector_number TEXT,
    rarity           TEXT,
    mana_cost        TEXT,
    type_line        TEXT,
    oracle_text      TEXT,
    colors           TEXT,    -- JSON array e.g. '["W","U"]'
    color_identity   TEXT,    -- JSON array
    power            TEXT,
    toughness        TEXT,
    is_token         INTEGER, -- 0/1
    is_rebalanced    INTEGER, -- 0/1
    image_uri        TEXT
);

CREATE TABLE IF NOT EXISTS card_decks (
    arena_id  INTEGER NOT NULL REFERENCES cards(arena_id),
    deck_name TEXT    NOT NULL,
    PRIMARY KEY (arena_id, deck_name)
);

-- Persists granted card events across log rotations.
-- (arena_id, source_id) is unique so re-running never double-counts.
CREATE TABLE IF NOT EXISTS granted_cards (
    arena_id  INTEGER NOT NULL,
    source_id TEXT    NOT NULL,  -- event/course UUID from InventoryInfo.Changes
    quantity  INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (arena_id, source_id)
);

CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT
);
"""


def init_db(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.executescript(SCHEMA)
    conn.commit()
    return conn


def persist_grants(
    conn: sqlite3.Connection,
    grants: list[tuple[int, str, int]],
) -> int:
    """
    Insert granted card events into granted_cards table.
    Uses INSERT OR IGNORE so re-running on the same logs never double-counts.
    Returns number of new grant records inserted.
    """
    cur = conn.cursor()
    new_count = 0
    for arena_id, source_id, quantity in grants:
        cur.execute(
            "INSERT OR IGNORE INTO granted_cards (arena_id, source_id, quantity) VALUES (?,?,?)",
            (arena_id, source_id, quantity),
        )
        new_count += cur.rowcount
    conn.commit()
    return new_count


def populate_db(
    conn: sqlite3.Connection,
    untapped_collection: dict[int, int],
    deck_membership: dict[int, list[str]],
    card_lookup: dict,
    image_lookup: dict,
) -> tuple[int, int]:
    cur = conn.cursor()
    matched = unmatched = 0

    # Aggregate total granted quantities per card from the persisted table
    granted_totals: dict[int, int] = {}
    for row in conn.execute("SELECT arena_id, SUM(quantity) FROM granted_cards GROUP BY arena_id"):
        granted_totals[row[0]] = row[1]

    # All cards: union of Untapped collection and any granted-only cards
    all_arena_ids = set(untapped_collection) | set(granted_totals)

    for arena_id in all_arena_ids:
        quantity = untapped_collection.get(arena_id) or granted_totals.get(arena_id, 0)
        source = SOURCE_COLLECTION if arena_id in untapped_collection else SOURCE_GRANTED

        card = card_lookup.get(arena_id)
        image_uri = image_lookup.get(arena_id)

        if card:
            matched += 1
            cur.execute(
                "INSERT OR REPLACE INTO cards VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    arena_id,
                    quantity,
                    source,
                    card["name"],
                    card["set_code"],
                    card["collector_number"],
                    card["rarity"],
                    card["mana_cost"],
                    card["type_line"],
                    card["oracle_text"],
                    card["colors"],
                    card["color_identity"],
                    card["power"],
                    card["toughness"],
                    int(card["is_token"]),
                    int(card["is_rebalanced"]),
                    image_uri,
                ),
            )
        else:
            unmatched += 1
            cur.execute(
                "INSERT OR REPLACE INTO cards (arena_id, quantity, source, image_uri) VALUES (?,?,?,?)",
                (arena_id, quantity, source, image_uri),
            )

        for deck_name in deck_membership.get(arena_id, []):
            cur.execute("INSERT OR IGNORE INTO card_decks VALUES (?,?)", (arena_id, deck_name))

    cur.execute(
        "INSERT OR REPLACE INTO meta VALUES ('last_updated', ?)",
        (datetime.now(timezone.utc).isoformat(),),
    )
    cur.execute("INSERT OR REPLACE INTO meta VALUES ('total_unique_cards', ?)", (str(len(all_arena_ids)),))
    cur.execute("INSERT OR REPLACE INTO meta VALUES ('matched_from_card_db', ?)", (str(matched),))

    conn.commit()
    return matched, unmatched


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Build MTGA collection SQLite DB from Untapped collection JSON."
    )
    parser.add_argument("--collection", metavar="PATH",
                        help="Untapped collection JSON (auto-detected)")
    parser.add_argument("--log", metavar="PATH", help="Path to MTGA Player.log (auto-detected)")
    parser.add_argument("--db", metavar="PATH", default="mtga_collection.db",
                        help="Output SQLite file (default: mtga_collection.db)")
    parser.add_argument("--card-db", metavar="PATH",
                        help="Path to Raw_CardDatabase_*.mtga (auto-detected)")
    parser.add_argument("--scryfall-cache", metavar="PATH", default=str(SCRYFALL_CACHE_PATH),
                        help="Scryfall cache file for image URIs")
    parser.add_argument("--no-images", action="store_true",
                        help="Skip Scryfall image URI lookup")
    args = parser.parse_args()

    # -- Untapped collection --
    collection_path = Path(args.collection) if args.collection else find_collection()
    if not collection_path or not collection_path.exists():
        print(
            "ERROR: Could not find Untapped collection JSON. Use --collection to specify its path.\n"
            "Run MTGA with the patched Untapped companion to generate it.",
            file=sys.stderr,
        )
        sys.exit(1)
    print(f"Collection: {collection_path}")
    untapped_collection = parse_untapped_collection(collection_path)
    print(f"  {len(untapped_collection):,} unique cards, {sum(untapped_collection.values()):,} total copies")

    # -- Log (optional — for deck membership and grant events) --
    log_path = Path(args.log) if args.log else find_log()
    deck_membership: dict[int, list[str]] = {}
    if log_path and log_path.exists():
        print(f"Log:        {log_path}")
        data = parse_log(log_path)
        if data:
            deck_membership = extract_deck_membership(data)
            user_deck_count = len({d for decks in deck_membership.values() for d in decks})
            print(f"  {user_deck_count:,} user decks, {len(deck_membership):,} cards with deck associations")
        else:
            print("  WARNING: No StartHook payload found in log — deck associations unavailable.")
    else:
        print("Log:        not found — deck associations will be empty")

    # -- CardDatabase --
    card_db_path = Path(args.card_db) if args.card_db else find_card_db()
    if not card_db_path or not card_db_path.exists():
        print(
            "ERROR: Could not find Raw_CardDatabase_*.mtga. Use --card-db to specify its path.",
            file=sys.stderr,
        )
        sys.exit(1)
    print(f"Card DB:    {card_db_path}")
    print("Loading card data…")
    card_lookup = build_card_lookup(card_db_path)

    # -- Scryfall images (optional) --
    image_lookup: dict[int, str] = {}
    if not args.no_images:
        print("Loading Scryfall images…")
        try:
            image_lookup = load_scryfall_images(Path(args.scryfall_cache))
        except Exception as e:
            print(f"  WARNING: Scryfall image lookup failed ({e}). Continuing without images.")

    # -- Parse GrantedCards from current + previous log --
    all_grants: list[tuple[int, str, int]] = []
    if log_path and log_path.exists():
        print("Scanning logs for GrantedCards events…")
        for log_file in [log_path, log_path.with_name("Player-prev.log")]:
            if log_file.exists():
                grants = parse_grants_from_log(log_file)
                all_grants.extend(grants)
                if grants:
                    print(f"  {log_file.name}: {len(grants)} grant records found")

    # -- Write DB --
    conn = init_db(args.db)

    new_grant_rows = persist_grants(conn, all_grants)
    if new_grant_rows:
        total_grant_rows = conn.execute("SELECT COUNT(*) FROM granted_cards").fetchone()[0]
        print(f"  {new_grant_rows} new grant records persisted ({total_grant_rows} total)")

    matched, unmatched = populate_db(conn, untapped_collection, deck_membership, card_lookup, image_lookup)
    total_cards = conn.execute("SELECT COUNT(*) FROM cards").fetchone()[0]
    conn.close()

    print(f"\nDone → {args.db}")
    print(f"  {total_cards:,} unique cards")
    print(f"  {matched:,} resolved from CardDatabase")
    if unmatched:
        print(f"  {unmatched:,} arena IDs not found in CardDatabase")


if __name__ == "__main__":
    main()
