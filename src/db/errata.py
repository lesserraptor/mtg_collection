"""errata.py — Apply known permanent corrections to the cards table.

Two strategies:

  1. Systematic basic-land fix: Scryfall's all_cards has off-by-one arena_id
     assignments for basic lands, causing e.g. Forest rows to have
     type_line='Basic Land — Mountain' and color_identity='["R"]'. Fixed
     using name-based WHERE clauses across all 57 affected sets (~180 rows).

  2. Hardcoded per-field overrides (KNOWN_ERRATA): Permanent data corrections
     that cannot be derived from Scryfall — specifically basic lands whose
     scryfall_id was assigned to the wrong arena_id, causing wrong images and
     type lines even after bulk enrichment. Seeded into the errata table on
     every apply_errata() call so corrections survive machine moves.

     NOTE: Set-code-mismatch cards (Alchemy year sets, SPG, Final Fantasy etc.)
     no longer need errata — tier 4 of ingest_scryfall() handles them by
     name-matching any Arena card still lacking a scryfall_id after tiers 1-3.

  3. Table-driven per-field overrides: Any additional rows in the errata table
     not covered by KNOWN_ERRATA. Populated manually for one-off fixes.

apply_errata() is called at the end of the ingest_cli main() so all known
corrections survive every data reload (MTGA card DB, Scryfall, log ingest).
"""

import sqlite3

BASIC_LAND_CORRECTIONS = [
    ("Plains",   "Basic Land \u2014 Plains",   '["W"]'),
    ("Island",   "Basic Land \u2014 Island",   '["U"]'),
    ("Swamp",    "Basic Land \u2014 Swamp",    '["B"]'),
    ("Mountain", "Basic Land \u2014 Mountain", '["R"]'),
    ("Forest",   "Basic Land \u2014 Forest",   '["G"]'),
]

# Permanent per-field overrides. Each tuple: (arena_id, field, new_value, note).
# Seeded into the errata table on every apply_errata() call.
#
# Only contains corrections that ingest cannot auto-fix: basic lands whose
# scryfall_id was assigned off-by-one in Scryfall's data, causing wrong images
# and type lines. Set-code-mismatch cards are handled by tier 4 of ingest_scryfall.
KNOWN_ERRATA = [
    # Basic lands with wrong scryfall_id assignment (off-by-one arena_id in Scryfall)
    (58441, "scryfall_id",      "9c010ca5-b609-4ae9-8c23-adf5723a9daa",                                              "basic land scryfall_id fix"),
    (58441, "image_uri_front",  "https://cards.scryfall.io/normal/front/9/c/9c010ca5-b609-4ae9-8c23-adf5723a9daa.jpg?1562790999", "basic land image fix"),
    (58443, "scryfall_id",      "0ac65c53-7b35-4ba0-9344-b311eee087cd",                                              "basic land scryfall_id fix"),
    (58443, "image_uri_front",  "https://cards.scryfall.io/normal/front/0/a/0ac65c53-7b35-4ba0-9344-b311eee087cd.jpg?1562782324", "basic land image fix"),
    (58449, "scryfall_id",      "802b1bb0-6c73-481a-ac3e-7d4e1682b4c2",                                              "basic land scryfall_id fix"),
    (58449, "image_uri_front",  "https://cards.scryfall.io/normal/front/8/0/802b1bb0-6c73-481a-ac3e-7d4e1682b4c2.jpg?1562789294", "basic land image fix"),
    (58453, "scryfall_id",      "1abe7f25-71c5-4fd2-8696-0a4ce8c4b0b6",                                              "basic land scryfall_id fix"),
    (58453, "image_uri_front",  "https://cards.scryfall.io/normal/front/1/a/1abe7f25-71c5-4fd2-8696-0a4ce8c4b0b6.jpg?1562783257", "basic land image fix"),
    (58453, "type_line",        "Basic Land \u2014 Forest",                                                           "basic land type_line fix"),
    (58453, "color_identity",   '["G"]',                                                                              "basic land color_identity fix"),
    (87453, "scryfall_id",      "4716a32c-91a6-470a-a686-d5eb3d27f46e",                                              "basic land scryfall_id fix"),
    (87453, "image_uri_front",  "https://cards.scryfall.io/normal/front/4/7/4716a32c-91a6-470a-a686-d5eb3d27f46e.jpg?1699045084", "basic land image fix"),
    (87453, "type_line",        "Basic Land \u2014 Plains",                                                           "basic land type_line fix"),
    (87453, "color_identity",   '["W"]',                                                                              "basic land color_identity fix"),
    (87455, "scryfall_id",      "a5f9dab5-4b13-4a98-8a64-76e69f0ba511",                                              "basic land scryfall_id fix"),
    (87455, "image_uri_front",  "https://cards.scryfall.io/normal/front/a/5/a5f9dab5-4b13-4a98-8a64-76e69f0ba511.jpg?1699045093", "basic land image fix"),
    (87457, "scryfall_id",      "4f893107-84b0-4e3f-b1f5-b04632f192c5",                                              "basic land scryfall_id fix"),
    (87457, "image_uri_front",  "https://cards.scryfall.io/normal/front/4/f/4f893107-84b0-4e3f-b1f5-b04632f192c5.jpg?1699045094", "basic land image fix"),
    (87459, "scryfall_id",      "7cb82fdb-5090-45c0-ae67-4846667c8625",                                              "basic land scryfall_id fix"),
    (87459, "image_uri_front",  "https://cards.scryfall.io/normal/front/7/c/7cb82fdb-5090-45c0-ae67-4846667c8625.jpg?1699044724", "basic land image fix"),
]

_ALLOWED_FIELDS = {
    "name", "mana_cost", "cmc", "type_line", "oracle_text", "rarity",
    "set_code", "collector_number", "colors", "color_identity", "keywords",
    "layout", "image_uri_front", "image_uri_back", "scryfall_id",
    "is_rebalanced",
}


def apply_errata(conn: sqlite3.Connection) -> int:
    """Apply all known errata to the cards table.

    Strategy 1: Fix basic-land type_line and color_identity using name-based
    WHERE clauses. Covers ~180 rows across 57 sets where Scryfall's off-by-one
    arena_id assignment caused wrong land type data.

    Strategy 2: Seed KNOWN_ERRATA into the errata table (INSERT OR REPLACE),
    then apply all table rows as targeted UPDATEs. Seeding ensures corrections
    survive machine moves without manual re-entry.

    Strategy 3: Apply any additional rows in the errata table not covered by
    KNOWN_ERRATA (manually inserted one-off fixes).

    Args:
        conn: Open sqlite3.Connection with the cards and errata tables present.

    Returns:
        Total number of card rows corrected.
    """
    corrected = 0

    # Strategy 1 — Systematic basic-land fix
    for name, correct_type, correct_ci in BASIC_LAND_CORRECTIONS:
        cursor = conn.execute(
            """UPDATE cards
               SET type_line = ?, color_identity = ?
               WHERE name = ?
                 AND (type_line != ? OR color_identity != ?)
                 AND type_line LIKE 'Basic Land%'""",
            (correct_type, correct_ci, name, correct_type, correct_ci),
        )
        corrected += cursor.rowcount

    # Strategy 2 — Seed KNOWN_ERRATA into the table so it's always present
    conn.executemany(
        "INSERT OR REPLACE INTO errata (arena_id, field, new_value, note) VALUES (?,?,?,?)",
        KNOWN_ERRATA,
    )
    conn.commit()

    # Strategy 2 & 3 — Apply all errata table rows (known + manual)
    errata_rows = conn.execute(
        "SELECT arena_id, field, new_value FROM errata"
    ).fetchall()

    for arena_id, field, new_value in errata_rows:
        if field not in _ALLOWED_FIELDS:
            continue
        cursor = conn.execute(
            f"UPDATE cards SET {field} = ? WHERE arena_id = ?",
            (new_value, arena_id),
        )
        corrected += cursor.rowcount

    conn.commit()
    return corrected
