"""Deck data layer for the MTGA collection browser.

Provides Arena text parsing, card name resolution, and deck CRUD operations.
"""

import re
import sqlite3
from datetime import datetime

SECTION_HEADERS = {'about', 'deck', 'sideboard', 'commander', 'companion'}
SECTION_MAP = {
    'deck': 'mainboard',
    'sideboard': 'sideboard',
    'commander': 'commander',
    'companion': 'companion',
}

# Matches: "4 Lightning Bolt (FCA) 55" or "4 Lightning Bolt"
CARD_LINE_RE = re.compile(
    r'^(\d+)\s+(.+?)(?:\s+\(([A-Z0-9a-z]+)\)\s+(\S+))?\s*$'
)


def parse_arena_decklist(text: str) -> dict:
    """Parse Arena text decklist into structured data.

    Returns:
        {
            'name': str or None,
            'lines': [
                {'quantity': int, 'name': str, 'set_code': str|None,
                 'collector_number': str|None, 'section': str}
            ]
        }
    """
    result = {'name': None, 'lines': []}
    current_section = 'mainboard'
    in_about = False

    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue

        lower = line.lower()

        # Section header detection
        if lower in SECTION_HEADERS:
            in_about = (lower == 'about')
            current_section = SECTION_MAP.get(lower, current_section)
            continue

        # About section: extract deck name
        if in_about and lower.startswith('name '):
            result['name'] = line[5:].strip()
            continue

        # Card line
        m = CARD_LINE_RE.match(line)
        if m:
            qty = int(m.group(1))
            name = m.group(2).strip()
            set_code = m.group(3)
            collector_number = m.group(4)
            result['lines'].append({
                'quantity': qty,
                'name': name,
                'set_code': set_code,
                'collector_number': collector_number,
                'section': current_section,
            })

    return result


def resolve_card_name(db, name: str, set_code: str = None, collector_number: str = None):
    """Resolve Arena card name to best arena_id. Returns None if not found."""
    # Strip DFC back face for matching input
    front_name = name.split(' // ')[0].strip()

    # Phase 1: exact set+collector match
    if set_code and collector_number:
        row = db.execute(
            "SELECT arena_id FROM cards WHERE LOWER(set_code)=LOWER(?) AND collector_number=?",
            [set_code, collector_number]
        ).fetchone()
        if row:
            return row['arena_id']

    # Phase 2: best-owned exact name match (full name or front-face match)
    row = db.execute("""
        SELECT c.arena_id
        FROM cards c
        LEFT JOIN collection col ON col.arena_id = c.arena_id
        WHERE LOWER(c.name) = LOWER(?)
           OR LOWER(c.name) LIKE LOWER(?) || ' //%'
        ORDER BY COALESCE(col.quantity, 0) DESC, c.arena_id ASC
        LIMIT 1
    """, [front_name, front_name]).fetchone()
    return row['arena_id'] if row else None


def import_deck(db, text: str, is_potential: bool = False, is_saved: bool = False) -> int:
    """Parse and import an Arena text decklist into the database.

    Returns:
        deck_id (int) of the newly inserted deck.
    """
    parsed = parse_arena_decklist(text)

    # Determine deck name: use parsed name, or generate Untitled-N
    if parsed['name']:
        deck_name = parsed['name']
    else:
        row = db.execute(
            "SELECT COUNT(*) AS n FROM decks WHERE name LIKE 'Untitled-%'"
        ).fetchone()
        deck_name = f"Untitled-{row['n'] + 1}"

    cursor = db.execute(
        "INSERT INTO decks (name, format, imported_at, source, is_potential, is_saved) VALUES (?, ?, ?, ?, ?, ?)",
        [deck_name, None, datetime.utcnow().isoformat(), 'arena', 1 if is_potential else 0, 1 if is_saved else 0]
    )
    deck_id = cursor.lastrowid

    for line in parsed['lines']:
        arena_id = resolve_card_name(db, line['name'], line['set_code'], line['collector_number'])
        db.execute(
            "INSERT INTO deck_lines (deck_id, arena_id, card_name, quantity, section) VALUES (?, ?, ?, ?, ?)",
            [deck_id, arena_id, line['name'], line['quantity'], line['section']]
        )

    db.commit()
    return deck_id


_ORDER_BY = {
    "name":         "d.name COLLATE NOCASE ASC",
    "name_desc":    "d.name COLLATE NOCASE DESC",
    "format":       "COALESCE(d.format,'~') ASC, d.name COLLATE NOCASE ASC",
    "format_desc":  "COALESCE(d.format,'~') DESC, d.name COLLATE NOCASE ASC",
    "cards":        "total_cards DESC, d.name COLLATE NOCASE ASC",
    "cards_asc":    "total_cards ASC, d.name COLLATE NOCASE ASC",
    "imported":     "d.imported_at DESC",
    "imported_asc": "d.imported_at ASC",
}


def list_decks(db, is_potential: bool = False, is_saved: bool = False, sort: str = "name") -> list:
    """Return all decks with ownership summary stats.

    Modes (mutually exclusive):
        is_saved=True  → saved decks (is_saved=1)
        is_potential=True → potential decks (is_potential=1, is_saved=0)
        both False → arena decks (is_potential=0, is_saved=0)
    """
    if is_saved:
        where = "d.is_saved = 1"
    elif is_potential:
        where = "d.is_potential = 1 AND d.is_saved = 0"
    else:
        where = "d.is_potential = 0 AND d.is_saved = 0"

    order_clause = _ORDER_BY.get(sort, _ORDER_BY["name"])

    rows = db.execute(f"""
        SELECT
            d.id,
            d.name,
            d.format,
            d.is_potential,
            d.is_saved,
            d.imported_at,
            COUNT(dl.id) AS total_lines,
            SUM(dl.quantity) AS total_cards,
            SUM(MIN(dl.quantity, CASE
                WHEN c.type_line LIKE '%Basic Land%' THEN dl.quantity
                ELSE (
                    SELECT CASE WHEN COALESCE(SUM(col.quantity), 0) >= 4 THEN dl.quantity
                                ELSE COALESCE(SUM(col.quantity), 0) END
                    FROM cards c2
                    JOIN collection col ON col.arena_id = c2.arena_id
                    WHERE LOWER(c2.name) = LOWER(COALESCE(c.name, dl.card_name))
                )
            END)) AS owned_cards,
            (SELECT COUNT(*) FROM deck_versions dv WHERE dv.deck_id = d.id) AS version_count
        FROM decks d
        LEFT JOIN deck_lines dl ON dl.deck_id = d.id AND dl.section = 'mainboard'
        LEFT JOIN cards c ON c.arena_id = dl.arena_id
        WHERE {where}
        GROUP BY d.id
        ORDER BY {order_clause}
    """).fetchall()
    return [dict(row) for row in rows]


def get_deck(db, deck_id: int):
    """Return a single deck row by id, or None if not found."""
    row = db.execute(
        "SELECT id, name, format, imported_at, source, is_potential, is_saved FROM decks WHERE id=?",
        [deck_id]
    ).fetchone()
    return dict(row) if row else None


def get_deck_lines(db, deck_id: int) -> list:
    """Return all lines for a deck with per-name owned quantity aggregated across printings.

    Returns:
        List of dicts with id, card_name, required, section, arena_id, total_owned.
    """
    rows = db.execute("""
        SELECT
            dl.id,
            dl.card_name,
            dl.quantity,
            dl.quantity        AS required,
            dl.section,
            dl.arena_id,
            COALESCE(
                (SELECT c2.arena_id FROM cards c2
                 JOIN collection col ON col.arena_id = c2.arena_id
                 WHERE (c2.arena_id = dl.arena_id
                        OR (dl.arena_id IS NULL AND LOWER(c2.name) = LOWER(dl.card_name)))
                   AND col.quantity > 0
                 ORDER BY col.quantity DESC, c2.arena_id ASC
                 LIMIT 1),
                dl.arena_id
            )                  AS display_arena_id,
            c.type_line,
            c.mana_cost,
            c.rarity,
            CASE
                WHEN c.type_line LIKE '%Basic Land%' THEN dl.quantity
                ELSE (
                    SELECT CASE WHEN COALESCE(SUM(col.quantity), 0) >= 4 THEN dl.quantity
                                ELSE COALESCE(SUM(col.quantity), 0) END
                    FROM cards c2
                    JOIN collection col ON col.arena_id = c2.arena_id
                    WHERE LOWER(c2.name) = LOWER(COALESCE(c.name, dl.card_name))
                )
            END                AS total_owned
        FROM deck_lines dl
        LEFT JOIN cards c ON c.arena_id = dl.arena_id
        WHERE dl.deck_id = ?
        ORDER BY
            CASE dl.section
                WHEN 'commander'  THEN 1
                WHEN 'companion'  THEN 2
                WHEN 'mainboard'  THEN 3
                WHEN 'sideboard'  THEN 4
                ELSE 5
            END,
            dl.card_name
    """, [deck_id]).fetchall()
    return [dict(row) for row in rows]


def export_deck_to_arena(db, deck_id: int) -> str:
    """Generate Arena-compatible text from stored deck_lines.

    Returns:
        Multi-section Arena text string.
    """
    deck = db.execute("SELECT name FROM decks WHERE id=?", [deck_id]).fetchone()
    lines_by_section = {'mainboard': [], 'sideboard': [], 'commander': [], 'companion': []}

    rows = db.execute("""
        SELECT dl.quantity, dl.card_name, dl.section,
               c.name as resolved_name, c.set_code, c.collector_number
        FROM deck_lines dl
        LEFT JOIN cards c ON c.arena_id = dl.arena_id
        WHERE dl.deck_id = ?
        ORDER BY dl.section, dl.card_name
    """, [deck_id]).fetchall()

    for row in rows:
        # Use resolved card name (full "Front // Back") if available, else stored name
        name = row['resolved_name'] or row['card_name']
        if row['set_code'] and row['collector_number']:
            line = f"{row['quantity']} {name} ({row['set_code'].upper()}) {row['collector_number']}"
        else:
            line = f"{row['quantity']} {name}"
        lines_by_section[row['section']].append(line)

    parts = []
    parts.append(f"About\nName {deck['name']}\n")
    if lines_by_section['mainboard']:
        parts.append("Deck\n" + "\n".join(lines_by_section['mainboard']))
    if lines_by_section['sideboard']:
        parts.append("Sideboard\n" + "\n".join(lines_by_section['sideboard']))
    if lines_by_section['commander']:
        parts.append("Commander\n" + "\n".join(lines_by_section['commander']))
    if lines_by_section['companion']:
        parts.append("Companion\n" + "\n".join(lines_by_section['companion']))
    return "\n\n".join(parts)


def update_deck_line_qty(db, line_id: int, quantity: int) -> None:
    """Update quantity for a deck line. If quantity <= 0, removes the line."""
    if quantity <= 0:
        db.execute("DELETE FROM deck_lines WHERE id=?", [line_id])
    else:
        db.execute("UPDATE deck_lines SET quantity=? WHERE id=?", [quantity, line_id])
    db.commit()


def remove_deck_line(db, line_id: int) -> None:
    """Delete a deck line by id."""
    db.execute("DELETE FROM deck_lines WHERE id=?", [line_id])
    db.commit()


def add_deck_line(db, deck_id: int, card_name: str, quantity: int, section: str = "mainboard", arena_id: int | None = None) -> dict | None:
    """Add a new line to a deck. Resolves card name to arena_id.

    Returns a dict with the new line data, or None if the card is not found.
    """
    if arena_id is None:
        arena_id = resolve_card_name(db, card_name)
    if not arena_id:
        return None
    cursor = db.execute(
        "INSERT INTO deck_lines (deck_id, arena_id, card_name, quantity, section) VALUES (?, ?, ?, ?, ?)",
        [deck_id, arena_id, card_name, quantity, section]
    )
    db.commit()
    return {"id": cursor.lastrowid, "deck_id": deck_id, "arena_id": arena_id,
            "card_name": card_name, "quantity": quantity, "section": section}


def delete_deck(db, deck_id: int) -> None:
    """Delete a deck and all its lines (cascade via ON DELETE CASCADE)."""
    db.execute("DELETE FROM decks WHERE id=?", [deck_id])
    db.commit()


def replace_deck_from_text(db, deck_id: int, text: str) -> None:
    """Replace all lines in a deck by re-parsing Arena text. Updates name if present."""
    parsed = parse_arena_decklist(text)
    db.execute("DELETE FROM deck_lines WHERE deck_id = ?", [deck_id])
    for line in parsed["lines"]:
        arena_id = resolve_card_name(db, line["name"], line["set_code"], line["collector_number"])
        db.execute(
            "INSERT INTO deck_lines (deck_id, arena_id, card_name, quantity, section) VALUES (?, ?, ?, ?, ?)",
            [deck_id, arena_id, line["name"], line["quantity"], line["section"]],
        )
    if parsed["name"]:
        db.execute("UPDATE decks SET name = ? WHERE id = ?", [parsed["name"], deck_id])
    db.commit()


def copy_deck_to_saved(db, deck_id: int) -> int:
    """Create an editable saved-deck copy of any deck (e.g. an arena log deck).

    Returns the new deck_id.
    """
    original = db.execute(
        "SELECT name, format FROM decks WHERE id = ?", [deck_id]
    ).fetchone()
    cursor = db.execute(
        "INSERT INTO decks (name, format, imported_at, source, is_potential, is_saved) VALUES (?, ?, ?, ?, ?, ?)",
        [original["name"], original["format"], datetime.utcnow().isoformat(), "arena", 0, 1],
    )
    new_id = cursor.lastrowid
    db.execute(
        """INSERT INTO deck_lines (deck_id, arena_id, card_name, quantity, section)
           SELECT ?, arena_id, card_name, quantity, section FROM deck_lines WHERE deck_id = ?""",
        [new_id, deck_id],
    )
    old_versions = db.execute(
        "SELECT id, version_num, label, created_at, source FROM deck_versions WHERE deck_id = ? ORDER BY version_num",
        [deck_id]
    ).fetchall()
    for v in old_versions:
        cursor = db.execute(
            "INSERT INTO deck_versions (deck_id, version_num, label, created_at, source) VALUES (?, ?, ?, ?, ?)",
            [new_id, v["version_num"], v["label"], v["created_at"], v["source"]]
        )
        new_version_id = cursor.lastrowid
        db.execute(
            """INSERT INTO deck_version_lines (version_id, arena_id, card_name, quantity, section)
               SELECT ?, arena_id, card_name, quantity, section FROM deck_version_lines WHERE version_id = ?""",
            [new_version_id, v["id"]]
        )
    db.commit()
    return new_id


def bulk_delete(db, deck_ids: list[int]) -> None:
    """Delete multiple decks."""
    db.executemany("DELETE FROM decks WHERE id=?", [(i,) for i in deck_ids])
    db.commit()


def bulk_move_to_saved(db, deck_ids: list[int]) -> None:
    """Move multiple decks to saved (is_potential=0, is_saved=1)."""
    db.executemany(
        "UPDATE decks SET is_potential=0, is_saved=1 WHERE id=?",
        [(i,) for i in deck_ids]
    )
    db.commit()


def bulk_move_to_potential(db, deck_ids: list[int]) -> None:
    """Move multiple decks to potential (is_potential=1, is_saved=0)."""
    db.executemany(
        "UPDATE decks SET is_potential=1, is_saved=0 WHERE id=?",
        [(i,) for i in deck_ids]
    )
    db.commit()


def rename_deck(db, deck_id: int, name: str) -> None:
    """Rename a deck."""
    db.execute("UPDATE decks SET name=? WHERE id=?", [name.strip(), deck_id])
    db.commit()


def update_deck_format(db, deck_id: int, format_value: str | None) -> None:
    """Update the format/deck-type field for a deck."""
    db.execute("UPDATE decks SET format=? WHERE id=?", [format_value or None, deck_id])
    db.commit()


def save_for_later(db, deck_id: int) -> None:
    """Mark an arena deck as saved (is_saved=1). Keeps is_potential=0."""
    db.execute("UPDATE decks SET is_saved=1 WHERE id=?", [deck_id])
    db.commit()


def move_to_saved(db, deck_id: int) -> None:
    """Move a potential deck to saved (is_potential=0, is_saved=1)."""
    db.execute("UPDATE decks SET is_potential=0, is_saved=1 WHERE id=?", [deck_id])
    db.commit()


def unsave_deck(db, deck_id: int) -> None:
    """Remove saved flag — deck returns to Potential Decks."""
    db.execute("UPDATE decks SET is_saved=0, is_potential=1 WHERE id=?", [deck_id])
    db.commit()


def get_deck_version(db, version_id: int) -> dict | None:
    """Return a single deck version row by id, or None if not found."""
    row = db.execute(
        "SELECT id, deck_id, version_num, label, created_at, source FROM deck_versions WHERE id = ?",
        [version_id]
    ).fetchone()
    return dict(row) if row else None


def get_deck_versions(db, deck_id: int) -> list:
    """Return all versions for a deck, newest first."""
    rows = db.execute("""
        SELECT id, version_num, label, created_at, source
        FROM deck_versions
        WHERE deck_id = ?
        ORDER BY version_num DESC
    """, [deck_id]).fetchall()
    return [dict(row) for row in rows]


def get_version_lines(db, version_id: int) -> list:
    """Return all lines for a deck version snapshot.

    Mirrors get_deck_lines() but queries deck_version_lines.
    total_owned/is_missing/display_owned are hardcoded 0 — version views are read-only snapshots.
    """
    rows = db.execute("""
        SELECT
            dvl.id,
            dvl.card_name,
            dvl.quantity,
            dvl.quantity AS required,
            dvl.section,
            dvl.arena_id,
            COALESCE(
                (SELECT c2.arena_id FROM cards c2
                 JOIN collection col ON col.arena_id = c2.arena_id
                 WHERE (c2.arena_id = dvl.arena_id
                        OR (dvl.arena_id IS NULL AND LOWER(c2.name) = LOWER(dvl.card_name)))
                   AND col.quantity > 0
                 ORDER BY col.quantity DESC, c2.arena_id ASC
                 LIMIT 1),
                dvl.arena_id
            )                  AS display_arena_id,
            c.type_line,
            c.mana_cost,
            c.rarity,
            CASE
                WHEN c.type_line LIKE '%Basic Land%' THEN dvl.quantity
                ELSE (
                    SELECT CASE WHEN COALESCE(SUM(col.quantity), 0) >= 4 THEN dvl.quantity
                                ELSE COALESCE(SUM(col.quantity), 0) END
                    FROM cards c2
                    JOIN collection col ON col.arena_id = c2.arena_id
                    WHERE LOWER(c2.name) = LOWER(COALESCE(c.name, dvl.card_name))
                )
            END                AS total_owned
        FROM deck_version_lines dvl
        LEFT JOIN cards c ON c.arena_id = dvl.arena_id
        WHERE dvl.version_id = ?
        ORDER BY
            CASE dvl.section
                WHEN 'commander' THEN 1
                WHEN 'companion' THEN 2
                WHEN 'mainboard' THEN 3
                WHEN 'sideboard' THEN 4
                ELSE 5
            END,
            dvl.card_name
    """, [version_id]).fetchall()
    return [dict(row) for row in rows]


def restore_from_version(db, deck_id: int, version_id: int) -> None:
    """Snapshot current deck state, then replace deck_lines from a version snapshot."""
    from src.db.deck_scan import create_version
    # Step 1: snapshot current state so the restore is reversible (VERS-03)
    create_version(db, deck_id)
    # Step 2: replace deck_lines from target version
    db.execute("DELETE FROM deck_lines WHERE deck_id = ?", [deck_id])
    db.execute("""
        INSERT INTO deck_lines (deck_id, arena_id, card_name, quantity, section)
        SELECT ?, arena_id, card_name, quantity, section
        FROM deck_version_lines WHERE version_id = ?
    """, [deck_id, version_id])
    db.commit()


def _build_arena_text_from_log_deck(db, deck: dict) -> str | None:
    """Convert a parse_log_decks() result dict to Arena text format.

    Returns the Arena text string, or None if no mainboard cards resolve.
    Handles Brawl format: sideboard cards go under 'Commander' section.
    """
    fmt = deck.get("format") or ""
    is_brawl = "brawl" in fmt.lower()

    def _card_name(arena_id):
        row = db.execute(
            "SELECT name FROM cards WHERE arena_id = ?", (arena_id,)
        ).fetchone()
        return row["name"] if row else None

    main_lines = [
        f"{e['quantity']} {name}"
        for e in deck["mainboard"]
        if (name := _card_name(e["arena_id"]))
    ]
    if not main_lines:
        return None

    side_lines = [
        f"{e['quantity']} {name}"
        for e in deck.get("sideboard", [])
        if (name := _card_name(e["arena_id"]))
    ]

    commander_lines = [
        f"{e['quantity']} {name}"
        for e in deck.get("commander", [])
        if (name := _card_name(e["arena_id"]))
    ]

    arena_text = f"About\nName {deck['name']}\n\nDeck\n" + "\n".join(main_lines)
    if commander_lines:
        arena_text += f"\n\nCommander\n" + "\n".join(commander_lines)
    elif side_lines:
        section_header = "Commander" if is_brawl else "Sideboard"
        arena_text += f"\n\n{section_header}\n" + "\n".join(side_lines)

    return arena_text
