"""Card browsing route: filter, paginate, and serve card data.

DB diagnostic results (run 2026-03-29):
  keywords populated: YES — values like ["Trample"], ["Flashback"], ["Cycling"]
  set_code casing: lowercase throughout (e.g., 2x2, aer, afr, blb, bro)
"""

from __future__ import annotations

import math
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request

from src.web.routes._set_info import _SET_INFO, _ensure_set_info, MAJOR_SET_TYPES

router = APIRouter()

_filter_options_cache: dict | None = None


def invalidate_filter_options_cache() -> None:
    """Clear the cached filter options so they will be rebuilt on the next request."""
    global _filter_options_cache
    _filter_options_cache = None

# W/U/B/R/G are standard color codes stored in the JSON colors array.
# C = Colorless (colors = '[]'). M = Multicolor (json_array_length > 1).
VALID_COLORS = {"W", "U", "B", "R", "G", "C", "M"}
VALID_RARITIES = {"common", "uncommon", "rare", "mythic"}
VALID_PER_PAGE = {20, 50, 75, 100, 200}
VALID_SORT = {"alpha", "color", "color_alpha", "color_cmc", "color_owned", "cmc"}
# WUBRG order: W=0, U=1, B=2, R=3, G=4, multicolor=5, colorless=6
_COLOR_ORDER_SQL = """
    CASE
        WHEN json_array_length(c.colors) = 0 THEN 6
        WHEN json_array_length(c.colors) > 1 THEN 5
        WHEN EXISTS (SELECT 1 FROM json_each(c.colors) WHERE value = 'W') THEN 0
        WHEN EXISTS (SELECT 1 FROM json_each(c.colors) WHERE value = 'U') THEN 1
        WHEN EXISTS (SELECT 1 FROM json_each(c.colors) WHERE value = 'B') THEN 2
        WHEN EXISTS (SELECT 1 FROM json_each(c.colors) WHERE value = 'R') THEN 3
        WHEN EXISTS (SELECT 1 FROM json_each(c.colors) WHERE value = 'G') THEN 4
        ELSE 6
    END
"""

_ORDER_BY = {
    "alpha":       "c.name",
    "color":       f"({_COLOR_ORDER_SQL})",
    "color_alpha": f"({_COLOR_ORDER_SQL}), c.name",
    "color_cmc":   f"({_COLOR_ORDER_SQL}), c.cmc, c.name",
    "color_owned": f"({_COLOR_ORDER_SQL}), COALESCE(col.quantity, 0) DESC, c.name",
    "cmc":         "c.cmc, c.name",
}

# Color order SQL for grouped mode — uses the `colors` SELECT alias (aggregated
# representative colors) rather than c.colors, which would be an arbitrary row
# value from the group and could produce wrong sort order for cards with reprints.
_COLOR_ORDER_SQL_GROUPED = """
    CASE
        WHEN json_array_length(colors) = 0 THEN 6
        WHEN json_array_length(colors) > 1 THEN 5
        WHEN colors LIKE '%"W"%' THEN 0
        WHEN colors LIKE '%"U"%' THEN 1
        WHEN colors LIKE '%"B"%' THEN 2
        WHEN colors LIKE '%"R"%' THEN 3
        WHEN colors LIKE '%"G"%' THEN 4
        ELSE 6
    END
"""

# ORDER BY expressions for the grouped query.
# Uses _COLOR_ORDER_SQL_GROUPED (alias-based) not _COLOR_ORDER_SQL (c.colors-based).
# color_owned uses the owned_quantity alias (SUM per group) instead of col.quantity.
_ORDER_BY_GROUPED = {
    "alpha":       "name",
    "color":       f"({_COLOR_ORDER_SQL_GROUPED})",
    "color_alpha": f"({_COLOR_ORDER_SQL_GROUPED}), name",
    "color_cmc":   f"({_COLOR_ORDER_SQL_GROUPED}), cmc, name",
    "color_owned": f"({_COLOR_ORDER_SQL_GROUPED}), owned_quantity DESC, name",
    "cmc":         "cmc, name",
}


def build_card_query(
    name: str = "",
    colors: list[str] | None = None,
    rarity: list[str] | None = None,
    type_line: str = "",
    creature_type: str = "",
    cmc_min: float | None = None,
    cmc_max: float | None = None,
    set_code: str = "",
    keywords: list[str] | None = None,
    min_owned: int = 1,
    owned_copies: list[int] | None = None,
    copies_active: bool = False,
    sort: str = "alpha",
    oracle_text: str = "",
) -> tuple[str, list[Any]]:
    """Build a parameterized SQL query for card filtering.

    Returns (sql, params) tuple. sql includes ORDER BY but no LIMIT/OFFSET
    so the caller can run a COUNT(*) variant for pagination totals.

    Color and keyword filters use json_each EXISTS (not LIKE) for exact
    element matching against JSON array columns.

    When set_code is empty, all versions (standard + alchemy) are grouped into one
    tile per card name. Alchemy cards ("A-X") merge with their base card ("X").
    Owned quantity sums all versions, capped at 4.
    When set_code is specified, per-printing tiles are returned (existing behavior).
    """
    colors = [c for c in (colors or []) if c in VALID_COLORS]
    rarity = [r for r in (rarity or []) if r in VALID_RARITIES]
    keywords = [kw for kw in (keywords or []) if kw]

    # Build shared filter conditions (applied in both grouped and ungrouped branches).
    # set_code and is_rebalanced filtering are handled per-branch below.
    # min_owned is handled per-branch: HAVING in grouped mode, WHERE in per-printing mode.
    conditions: list[str] = []
    params: list[Any] = []

    if name:
        conditions.append("c.name LIKE ?")
        params.append(f"%{name}%")

    if oracle_text:
        conditions.append("c.oracle_text LIKE ?")
        params.append(f"%{oracle_text}%")

    # Separate special color codes from standard ones before building conditions.
    # C = Colorless (empty colors array). M = Multicolor (2+ colors).
    # Standard codes W/U/B/R/G use json_each EXISTS for AND semantics.
    special_colors = {c for c in colors if c in ("C", "M")}
    standard_colors = [c for c in colors if c not in special_colors]

    # Color conditions are kept separate from other conditions because grouped mode
    # must apply them as HAVING on aggregated colors, not WHERE on individual rows.
    # (A card with a colorless reprint would otherwise falsely match the colorless filter.)
    color_subconditions: list[str] = []
    color_params: list[Any] = []
    # Standard colors (W/U/B/R/G): exact color set match when no C/M mixed in.
    # "R only" → mono-red cards; "R+W" → exactly red+white. When C or M is also
    # selected, fall back to contains-semantics so e.g. M+R means "multicolor with red".
    if standard_colors:
        if not special_colors:
            color_subconditions.append(f"json_array_length(c.colors) = {len(standard_colors)}")
        for color in standard_colors:
            color_subconditions.append("EXISTS (SELECT 1 FROM json_each(c.colors) WHERE value = ?)")
            color_params.append(color)
    # Colorless: colors array is empty
    if "C" in special_colors:
        color_subconditions.append("json_array_length(c.colors) = 0")
    # Multicolor: 2 or more colors in array
    if "M" in special_colors:
        color_subconditions.append("json_array_length(c.colors) >= 2")

    if rarity:
        placeholders = ",".join("?" * len(rarity))
        conditions.append(f"c.rarity IN ({placeholders})")
        params.extend(rarity)

    if type_line:
        conditions.append("c.type_line LIKE ?")
        params.append(f"%{type_line}%")

    if creature_type:
        # Match subtype in the portion after '—' to avoid false positives
        conditions.append("c.type_line LIKE '%—%' AND SUBSTR(c.type_line, INSTR(c.type_line, '—') + 2) LIKE ?")
        params.append(f"%{creature_type}%")

    if cmc_min is not None:
        conditions.append("c.cmc >= ?")
        params.append(cmc_min)

    if cmc_max is not None:
        conditions.append("c.cmc <= ?")
        params.append(cmc_max)

    # JSON array: each keyword must be present (AND semantics)
    for kw in keywords:
        conditions.append("EXISTS (SELECT 1 FROM json_each(c.keywords) WHERE value = ?)")
        params.append(kw)

    order_by = _ORDER_BY.get(sort, _ORDER_BY["alpha"])

    if not set_code:
        # --- Grouped mode: no set filter active ---
        # All cards (standard + alchemy) are grouped by normalized name.
        # Alchemy cards ("A-X") are unified with their base card ("X") — one tile per name.
        # Owned quantity sums all versions and is capped at 4.
        # Non-alchemy values (arena_id, image, name) are preferred via CASE expressions.

        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

        # In grouped mode, color conditions apply to the representative (aggregated) colors
        # so that a card with one colorless reprint doesn't appear under colorless.
        _AGG_COLORS = "COALESCE(MIN(CASE WHEN c.is_rebalanced=0 THEN c.colors END), MIN(c.colors))"
        having_clauses: list[str] = []
        having_params: list[Any] = []
        if owned_copies:
            bucket_clauses = []
            for b in owned_copies:
                if b == 0:
                    bucket_clauses.append("SUM(COALESCE(col.quantity, 0)) = 0")
                elif b < 4:
                    bucket_clauses.append(f"SUM(COALESCE(col.quantity, 0)) = {b}")
                else:  # 4+
                    bucket_clauses.append("SUM(COALESCE(col.quantity, 0)) >= 4")
            if bucket_clauses:
                having_clauses.append("(" + " OR ".join(bucket_clauses) + ")")
        elif copies_active:
            having_clauses.append("1=0")
        elif min_owned > 0:
            having_clauses.append("SUM(COALESCE(col.quantity, 0)) >= ?")
            having_params.append(min_owned)
        if standard_colors:
            if not special_colors:
                having_clauses.append(f"json_array_length({_AGG_COLORS}) = {len(standard_colors)}")
            for color in color_params:
                having_clauses.append(f"{_AGG_COLORS} LIKE ?")
                having_params.append(f'%"{color}"%')
        if "C" in special_colors:
            having_clauses.append(f"json_array_length({_AGG_COLORS}) = 0")
        if "M" in special_colors:
            having_clauses.append(f"json_array_length({_AGG_COLORS}) >= 2")
        having = ("HAVING " + " AND ".join(having_clauses)) if having_clauses else ""

        grouped_order_by = _ORDER_BY_GROUPED.get(sort, _ORDER_BY_GROUPED["alpha"])
        sql = f"""
            WITH deck_demand AS (
                SELECT
                    LOWER(CASE WHEN instr(dl.card_name, ' // ') > 0
                               THEN substr(dl.card_name, 1, instr(dl.card_name, ' // ') - 1)
                               ELSE dl.card_name END) AS name_lower,
                    COUNT(DISTINCT dl.deck_id) AS deck_count
                FROM deck_lines dl
                JOIN decks d ON d.id = dl.deck_id AND (d.is_potential = 1 OR d.is_saved = 1)
                LEFT JOIN collection col ON col.arena_id = dl.arena_id
                WHERE dl.section = 'mainboard'
                  AND COALESCE(col.quantity, 0) < dl.quantity
                GROUP BY name_lower
            ),
            _owned_rep AS (
                SELECT LOWER(cr.name) AS name_key, cr.arena_id,
                       ROW_NUMBER() OVER (
                           PARTITION BY LOWER(cr.name)
                           ORDER BY colr.quantity DESC, cr.arena_id ASC
                       ) AS rn
                FROM cards cr
                JOIN collection colr ON colr.arena_id = cr.arena_id
                WHERE cr.is_rebalanced = 0 AND colr.quantity > 0
            ),
            _rep AS (SELECT name_key, arena_id FROM _owned_rep WHERE rn = 1)
            SELECT
                COALESCE(
                    _rep.arena_id,
                    MIN(CASE WHEN c.is_rebalanced = 0 THEN c.arena_id END),
                    MIN(c.arena_id)
                ) AS arena_id,
                COALESCE(
                    MIN(CASE WHEN c.is_rebalanced = 0 THEN c.name END),
                    MIN(c.name)
                ) AS name,
                COALESCE(MIN(CASE WHEN c.is_rebalanced=0 THEN c.mana_cost END), MIN(c.mana_cost)) AS mana_cost,
                COALESCE(MIN(CASE WHEN c.is_rebalanced=0 THEN c.type_line END), MIN(c.type_line)) AS type_line,
                COALESCE(MIN(CASE WHEN c.is_rebalanced=0 THEN c.rarity END), MIN(c.rarity)) AS rarity,
                NULL AS set_code,
                COALESCE(MIN(CASE WHEN c.is_rebalanced=0 THEN c.cmc END), MIN(c.cmc)) AS cmc,
                COALESCE(MIN(CASE WHEN c.is_rebalanced=0 THEN c.colors END), MIN(c.colors)) AS colors,
                COALESCE(MIN(CASE WHEN c.is_rebalanced=0 THEN c.image_uri_front END), MIN(c.image_uri_front)) AS image_uri_front,
                COALESCE(MIN(CASE WHEN c.is_rebalanced=0 THEN c.image_uri_back END), MIN(c.image_uri_back)) AS image_uri_back,
                COALESCE(MIN(CASE WHEN c.is_rebalanced=0 THEN c.layout END), MIN(c.layout)) AS layout,
                CASE WHEN SUM(COALESCE(col.quantity, 0)) > 4 THEN 4 ELSE SUM(COALESCE(col.quantity, 0)) END AS owned_quantity,
                0 AS is_rebalanced,
                COALESCE(MAX(dd.deck_count), 0) AS deck_demand
            FROM cards c
            LEFT JOIN collection col ON col.arena_id = c.arena_id
            LEFT JOIN _rep ON _rep.name_key = LOWER(c.name)
            LEFT JOIN deck_demand dd ON dd.name_lower = LOWER(c.name)
            {where}
            GROUP BY LOWER(c.name)
            {having}
            ORDER BY {grouped_order_by}
        """
        return sql, params + having_params

    else:
        # --- Per-printing mode: set filter active ---
        # Show one tile per arena_id exactly as before.
        if color_subconditions:
            conditions.extend(color_subconditions)
            params.extend(color_params)
        if owned_copies:
            bucket_clauses = []
            for b in owned_copies:
                if b == 0:
                    bucket_clauses.append("COALESCE(col.quantity, 0) = 0")
                elif b < 4:
                    bucket_clauses.append(f"COALESCE(col.quantity, 0) = {b}")
                else:
                    bucket_clauses.append("COALESCE(col.quantity, 0) >= 4")
            if bucket_clauses:
                conditions.append("(" + " OR ".join(bucket_clauses) + ")")
        elif copies_active:
            # User explicitly deselected all chips — show nothing
            conditions.append("1=0")
        elif min_owned > 0:
            conditions.append("COALESCE(col.quantity, 0) >= ?")
            params.append(min_owned)
        conditions.append("LOWER(c.set_code) = LOWER(?)")
        params.append(set_code)

        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

        sql = f"""
            WITH deck_demand AS (
                SELECT
                    LOWER(CASE WHEN instr(dl.card_name, ' // ') > 0
                               THEN substr(dl.card_name, 1, instr(dl.card_name, ' // ') - 1)
                               ELSE dl.card_name END) AS name_lower,
                    COUNT(DISTINCT dl.deck_id) AS deck_count
                FROM deck_lines dl
                JOIN decks d ON d.id = dl.deck_id AND (d.is_potential = 1 OR d.is_saved = 1)
                LEFT JOIN collection col ON col.arena_id = dl.arena_id
                WHERE dl.section = 'mainboard'
                  AND COALESCE(col.quantity, 0) < dl.quantity
                GROUP BY name_lower
            )
            SELECT
                c.arena_id,
                c.name,
                c.mana_cost,
                c.type_line,
                c.rarity,
                c.set_code,
                c.cmc,
                c.colors,
                c.image_uri_front,
                c.image_uri_back,
                c.layout,
                COALESCE(col.quantity, 0) AS owned_quantity,
                c.is_rebalanced,
                COALESCE(dd.deck_count, 0) AS deck_demand
            FROM cards c
            LEFT JOIN collection col ON col.arena_id = c.arena_id
            LEFT JOIN deck_demand dd ON dd.name_lower = LOWER(c.name)
            {where}
            ORDER BY {order_by}
        """
        return sql, params


def query_cards(
    db,
    page: int = 1,
    per_page: int = 20,
    **filter_kwargs,
) -> tuple[list[dict], int]:
    """Execute a filtered card query with pagination.

    Returns (cards, total_count) where cards is a list of dicts for the
    requested page and total_count is the total matching rows (for pagination).
    """
    per_page = per_page if per_page in VALID_PER_PAGE else 20
    page = max(1, page)
    offset = (page - 1) * per_page

    sql, params = build_card_query(**filter_kwargs)

    # Count query: wrap in subquery to count total rows
    count_sql = f"SELECT COUNT(*) FROM ({sql}) AS sub"
    total = db.execute(count_sql, params).fetchone()[0]

    # Data query: add pagination
    paginated_sql = sql + f" LIMIT {per_page} OFFSET {offset}"
    rows = db.execute(paginated_sql, params).fetchall()

    cards = [dict(row) for row in rows]
    return cards, total


def get_filter_options(db) -> dict:
    """Return distinct values for filter dropdowns.

    Returns dict with:
      sets: list of dicts with 'code' and 'name' keys, major sets only,
            most-recent-first (proxy: max arena_id DESC).
      keywords: list of distinct keyword strings from json_each(keywords), sorted.
               Empty list if keywords column is not populated.
      creature_types: sorted list of distinct creature subtypes extracted from
               type_line (the portion after '—' for Creature cards).

    Result is cached per-process; second call is a no-op.
    """
    global _filter_options_cache
    if _filter_options_cache is not None:
        return _filter_options_cache

    # Major sets only: is_rebalanced=0, no basics, >= 50 distinct cards, most-recent-first.
    # Further filtered to draftable/premier set types via Scryfall set_type (MAJOR_SET_TYPES).
    # The 50-card threshold drops paper-only bleed-through sets that Scryfall enrichment
    # can pull in (e.g. Ravnica Remastered with 10 signets) while keeping every real Arena
    # release including the smallest remasters and bonus expansions.
    # booster=1 is enforced only for sets that actually have a booster product (>= 50
    # booster cards) — matches the sets.py set tracker logic so the two views stay in
    # sync and Alchemy/commander sets whose cards all carry booster=0 still show up.
    _ensure_set_info()
    set_rows = db.execute("""
        WITH booster_sets AS (
            SELECT set_code FROM cards
            WHERE is_rebalanced = 0 AND rarity NOT IN ('basic') AND booster = 1
            GROUP BY set_code HAVING COUNT(*) >= 50
        )
        SELECT c.set_code, COUNT(DISTINCT c.arena_id) AS total_cards, MAX(c.arena_id) AS max_arena_id
        FROM cards c
        WHERE c.is_rebalanced = 0 AND c.rarity NOT IN ('basic')
          AND (c.booster = 1 OR c.set_code NOT IN (SELECT set_code FROM booster_sets))
        GROUP BY c.set_code
        HAVING total_cards >= 50
        ORDER BY max_arena_id DESC
    """).fetchall()
    sets = []
    for row in set_rows:
        code = row["set_code"].lower()
        info = _SET_INFO.get(code, {})
        if info.get("set_type") not in MAJOR_SET_TYPES:
            continue
        sets.append({"code": row["set_code"], "name": info.get("name") or code.upper()})

    # Distinct keywords from JSON array column
    keyword_rows = db.execute("""
        SELECT DISTINCT value AS kw
        FROM cards, json_each(cards.keywords)
        WHERE cards.keywords IS NOT NULL AND length(cards.keywords) > 2
        ORDER BY value
    """).fetchall()
    kw_list = [row["kw"] for row in keyword_rows]

    # Distinct creature subtypes: split type_line after '—' and collect words
    creature_rows = db.execute("""
        SELECT DISTINCT type_line FROM cards
        WHERE type_line LIKE '%Creature%' AND type_line LIKE '%—%'
    """).fetchall()
    subtypes: set[str] = set()
    for row in creature_rows:
        after_dash = row["type_line"].split("—", 1)[1]
        for word in after_dash.split():
            if word.strip():
                subtypes.add(word.strip())
    creature_types = sorted(subtypes)

    _filter_options_cache = {"sets": sets, "keywords": kw_list, "creature_types": creature_types}
    return _filter_options_cache


@router.get("/cards")
async def collection_view(
    request: Request,
    name: str = "",
    colors: list[str] = Query(default=[]),
    rarity: list[str] = Query(default=[]),
    type_line: str = "",
    creature_type: str = "",
    cmc_min_raw: str = Query(default="", alias="cmc_min"),
    cmc_max_raw: str = Query(default="", alias="cmc_max"),
    set_code: str = "",
    keywords: list[str] = Query(default=[]),
    sort: str = "alpha",
    min_owned: int = 1,
    owned_copies: list[int] = Query(default=[]),
    copies_active: bool = False,
    page: int = 1,
    per_page: int = 20,
    oracle_text: str = "",
):
    """Collection browsing view with filter support.

    Returns full page on first load, card_grid fragment on HTMX requests.
    """
    db = request.app.state.db

    # Apply user-configured defaults when params absent from URL
    if not request.query_params.get("sort"):
        _sort_default = db.execute(
            "SELECT value FROM meta WHERE key = 'collection_default_sort'"
        ).fetchone()
        if _sort_default:
            sort = _sort_default["value"]
    if not request.query_params.get("per_page"):
        _per_page_default = db.execute(
            "SELECT value FROM meta WHERE key = 'collection_default_per_page'"
        ).fetchone()
        if _per_page_default:
            try:
                per_page = int(_per_page_default["value"])
            except (ValueError, TypeError):
                pass

    cmc_min = float(cmc_min_raw) if cmc_min_raw else None
    cmc_max = float(cmc_max_raw) if cmc_max_raw else None

    # Treat cmc_max as exact match unless it's the "6+" sentinel (99).
    # Without this, selecting "1" would include CMC 0.
    if cmc_max is not None and cmc_min is None:
        if cmc_max == 99:
            cmc_min = 6.0
            cmc_max = None
        else:
            cmc_min = cmc_max

    sort = sort if sort in VALID_SORT else "alpha"

    filter_params = dict(
        name=name,
        colors=colors,
        rarity=rarity,
        type_line=type_line,
        creature_type=creature_type,
        cmc_min=cmc_min,
        cmc_max=cmc_max,
        set_code=set_code,
        keywords=keywords,
        sort=sort,
        min_owned=min_owned,
        owned_copies=owned_copies,
        copies_active=copies_active,
        oracle_text=oracle_text,
    )

    cards, total = query_cards(db, page=page, per_page=per_page, **filter_params)

    templates = request.app.state.templates
    filter_options = get_filter_options(db)
    per_page_safe = per_page if per_page in VALID_PER_PAGE else 20
    total_pages = max(1, math.ceil(total / per_page_safe))

    # Starlette 1.x TemplateResponse: request is positional arg 1, context is arg 3
    context = {
        "cards": cards,
        "total": total,
        "page": page,
        "per_page": per_page_safe,
        "total_pages": total_pages,
        "mode": "collection",
        # Filter state for pre-filling form
        "selected_colors": colors,
        "selected_rarity": rarity,
        "name": name,
        "type_line": type_line,
        "creature_type": creature_type,
        "cmc_min": cmc_min,
        "cmc_max": cmc_max,
        "set_code": set_code,
        "selected_keywords": keywords,
        "sort": sort,
        "min_owned": min_owned,
        "owned_copies": owned_copies,
        "copies_active": copies_active,
        "oracle_text": oracle_text,
        # Dropdown options
        "sets": filter_options["sets"],
        "all_keywords": filter_options["keywords"],
        "all_creature_types": filter_options["creature_types"],
    }

    if request.headers.get("HX-Request"):
        return templates.TemplateResponse(request, "partials/card_grid.html", context)
    return templates.TemplateResponse(request, "collection.html", context)


@router.get("/cards/{arena_id}/detail")
async def card_detail(request: Request, arena_id: int):
    """Return the card detail panel partial for a single card."""
    db = request.app.state.db
    row = db.execute(
        """
        SELECT
            c.arena_id, c.name, c.mana_cost, c.cmc, c.type_line, c.oracle_text,
            c.rarity, c.set_code, c.collector_number, c.colors,
            c.keywords, c.image_uri_front, c.image_uri_back, c.layout, c.is_rebalanced,
            COALESCE(col.quantity, 0) AS owned_quantity,
            MIN(
                (SELECT COALESCE(SUM(col2.quantity), 0)
                 FROM cards c2
                 LEFT JOIN collection col2 ON col2.arena_id = c2.arena_id
                 WHERE LOWER(c2.name) = LOWER(c.name)),
                4
            ) AS total_owned
        FROM cards c
        LEFT JOIN collection col ON col.arena_id = c.arena_id
        WHERE c.arena_id = ?
        """,
        [arena_id],
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail=f"Card {arena_id} not found")

    # Fetch all versions with same name for the version art strip (standard + alchemy).
    # is_rebalanced=1 cards use the same name as their standard counterpart (no "A-" prefix).
    printings = db.execute(
        """SELECT c.arena_id, c.set_code, c.collector_number, c.is_rebalanced,
                  c.image_uri_front,
                  COALESCE(col.quantity, 0) AS owned_quantity
           FROM cards c
           LEFT JOIN collection col ON col.arena_id = c.arena_id
           WHERE LOWER(c.name) = LOWER(?) AND COALESCE(col.quantity, 0) > 0
           ORDER BY c.is_rebalanced ASC, c.set_code, c.arena_id""",
        [row["name"]],
    ).fetchall()

    # Fetch decks (potential + saved) that want this card
    front_name = dict(row)["name"].split(" // ")[0].strip()
    deck_rows = db.execute("""
        SELECT DISTINCT d.id, d.name
        FROM deck_lines dl
        JOIN decks d ON d.id = dl.deck_id AND (d.is_potential = 1 OR d.is_saved = 1)
        LEFT JOIN collection col ON col.arena_id = COALESCE(dl.arena_id, ?)
        WHERE dl.section = 'mainboard'
          AND (
              LOWER(dl.card_name) = LOWER(?)
              OR LOWER(dl.card_name) LIKE LOWER(?) || ' //%'
              OR dl.arena_id = ?
          )
          AND COALESCE(col.quantity, 0) < dl.quantity
        ORDER BY d.name
    """, [arena_id, front_name, front_name, arena_id]).fetchall()
    card_decks = [dict(r) for r in deck_rows]

    templates = request.app.state.templates
    return templates.TemplateResponse(
        request, "partials/card_detail.html",
        {"card": dict(row), "printings": [dict(p) for p in printings], "card_decks": card_decks},
    )
