"""Analysis data layer for the MTGA collection browser.

Provides crafting priority query functions:
- get_missing_cards_ranked: which missing cards unlock the most potential/saved decks
- get_decks_ranked_by_missing: which potential/saved decks are closest to complete
"""

import sqlite3


_DFC_STRIP = """LOWER(CASE WHEN instr(c.name, ' // ') > 0
                          THEN substr(c.name, 1, instr(c.name, ' // ') - 1)
                          ELSE c.name END)"""

def _dfc_strip(col: str) -> str:
    """Return SQL expression that strips ' // back-face' from a card name column."""
    return (f"LOWER(CASE WHEN instr({col}, ' // ') > 0"
            f" THEN substr({col}, 1, instr({col}, ' // ') - 1)"
            f" ELSE {col} END)")

_OWNED_BY_NAME_CTE = f"""
owned_by_name AS (
    SELECT
        {_DFC_STRIP} AS name_lower,
        SUM(col.quantity) AS total_owned
    FROM cards c
    JOIN collection col ON col.arena_id = c.arena_id
    GROUP BY name_lower
)""".strip()

_CARD_RARITY_CTE = f"""
card_rarity AS (
    SELECT
        {_DFC_STRIP} AS name_lower,
        c.rarity,
        c.arena_id
    FROM cards c
    WHERE c.arena_id = (
        SELECT MIN(c2.arena_id) FROM cards c2
        WHERE LOWER(CASE WHEN instr(c2.name, ' // ') > 0
                         THEN substr(c2.name, 1, instr(c2.name, ' // ') - 1)
                         ELSE c2.name END)
              = LOWER(CASE WHEN instr(c.name, ' // ') > 0
                           THEN substr(c.name, 1, instr(c.name, ' // ') - 1)
                           ELSE c.name END)
    )
)""".strip()


def get_missing_cards_ranked(db: sqlite3.Connection) -> list:
    """Return missing cards ranked by how many potential/saved decks need them.

    Filters deck_lines to only potential and saved decks (Arena decks excluded).
    DFC cards are matched by front-face name stripping.

    Returns sqlite3.Row list with columns:
        card_name, rarity, copies_missing, deck_count, arena_id
    Sorted by deck_count DESC, copies_missing ASC.
    """
    sql = f"""
WITH {_OWNED_BY_NAME_CTE},
{_CARD_RARITY_CTE},
missing_lines AS (
    -- scoped to potential/saved decks only; excludes arena log decks and complete decks
    SELECT
        dl.card_name,
        dl.arena_id,
        dl.deck_id,
        MAX(0, dl.quantity - COALESCE(obn.total_owned, 0)) AS copies_missing
    FROM deck_lines dl
    JOIN decks d ON d.id = dl.deck_id AND (d.is_potential = 1 OR d.is_saved = 1)
    LEFT JOIN owned_by_name obn ON obn.name_lower = {_dfc_strip("dl.card_name")}
    WHERE dl.section = 'mainboard'
      AND dl.quantity > COALESCE(obn.total_owned, 0)  -- excludes cards fully owned; complete decks produce no rows
)
SELECT
    ml.card_name,
    COALESCE(cr.rarity, 'unknown') AS rarity,
    ml.copies_missing,
    COUNT(DISTINCT ml.deck_id) AS deck_count,
    COALESCE(cr.arena_id, ml.arena_id) AS arena_id,
    card.type_line
FROM missing_lines ml
LEFT JOIN card_rarity cr ON cr.name_lower = {_dfc_strip("ml.card_name")}
LEFT JOIN cards card ON card.arena_id = COALESCE(cr.arena_id, ml.arena_id)
GROUP BY LOWER(ml.card_name)
ORDER BY deck_count DESC, ml.copies_missing ASC
"""
    return db.execute(sql).fetchall()


def get_missing_cards_decks(db: sqlite3.Connection) -> dict:
    """Return a mapping from card_name (lowercase) to list of deck dicts.

    Each deck dict has keys: id, name.
    Only potential and saved decks are included (same filter as get_missing_cards_ranked).
    Used to render deck links alongside each missing card row.
    """
    sql = """
SELECT
    LOWER(CASE WHEN instr(dl.card_name, ' // ') > 0
               THEN substr(dl.card_name, 1, instr(dl.card_name, ' // ') - 1)
               ELSE dl.card_name END) AS card_name_lower,
    d.id,
    d.name
FROM deck_lines dl
JOIN decks d ON d.id = dl.deck_id AND (d.is_potential = 1 OR d.is_saved = 1)
LEFT JOIN (
    SELECT
        LOWER(CASE WHEN instr(c.name, ' // ') > 0
                   THEN substr(c.name, 1, instr(c.name, ' // ') - 1)
                   ELSE c.name END) AS name_lower,
        SUM(col.quantity) AS total_owned
    FROM cards c
    JOIN collection col ON col.arena_id = c.arena_id
    GROUP BY name_lower
) obn ON obn.name_lower = LOWER(CASE WHEN instr(dl.card_name, ' // ') > 0
                                     THEN substr(dl.card_name, 1, instr(dl.card_name, ' // ') - 1)
                                     ELSE dl.card_name END)
WHERE dl.section = 'mainboard'
  AND dl.quantity > COALESCE(obn.total_owned, 0)
ORDER BY d.name
"""
    result: dict[str, list] = {}
    for row in db.execute(sql).fetchall():
        key = row["card_name_lower"]
        if key not in result:
            result[key] = []
        result[key].append({"id": row["id"], "name": row["name"]})
    return result


def get_decks_ranked_by_missing(db: sqlite3.Connection) -> list:
    """Return potential and saved decks ranked by fewest missing mainboard cards.

    Decks with zero missing cards are excluded (they are not crafting targets).
    DFC cards are matched by front-face name stripping.

    Returns sqlite3.Row list with columns:
        id, name, distinct_missing, total_missing,
        wc_common, wc_uncommon, wc_rare, wc_mythic, wc_unknown
    Sorted by total_missing ASC.
    """
    sql = f"""
WITH {_OWNED_BY_NAME_CTE},
{_CARD_RARITY_CTE},
deck_missing AS (
    -- only rows where a card is not fully owned; decks with all cards owned produce no rows here
    SELECT
        dl.deck_id,
        dl.card_name,
        COALESCE(cr.rarity, 'unknown') AS rarity,
        MAX(0, dl.quantity - COALESCE(obn.total_owned, 0)) AS copies_missing
    FROM deck_lines dl
    JOIN decks d ON d.id = dl.deck_id AND (d.is_potential = 1 OR d.is_saved = 1)
    LEFT JOIN owned_by_name obn ON obn.name_lower = {_dfc_strip("dl.card_name")}
    LEFT JOIN card_rarity cr ON cr.name_lower = {_dfc_strip("dl.card_name")}
    WHERE dl.section = 'mainboard'
      AND dl.quantity > COALESCE(obn.total_owned, 0)
)
SELECT
    d.id,
    d.name,
    COUNT(DISTINCT LOWER(dm.card_name)) AS distinct_missing,
    SUM(dm.copies_missing)              AS total_missing,
    SUM(CASE WHEN dm.rarity = 'common'   THEN dm.copies_missing ELSE 0 END) AS wc_common,
    SUM(CASE WHEN dm.rarity = 'uncommon' THEN dm.copies_missing ELSE 0 END) AS wc_uncommon,
    SUM(CASE WHEN dm.rarity = 'rare'     THEN dm.copies_missing ELSE 0 END) AS wc_rare,
    SUM(CASE WHEN dm.rarity = 'mythic'   THEN dm.copies_missing ELSE 0 END) AS wc_mythic,
    SUM(CASE WHEN dm.rarity = 'unknown'  THEN dm.copies_missing ELSE 0 END) AS wc_unknown
FROM decks d
-- excludes complete decks (total_missing = 0); only potential decks with gaps appear in analysis
JOIN deck_missing dm ON dm.deck_id = d.id
WHERE d.is_potential = 1 OR d.is_saved = 1
GROUP BY d.id
HAVING total_missing > 0  -- explicit guard: complete decks never appear in analysis output
ORDER BY total_missing ASC
"""
    return db.execute(sql).fetchall()
