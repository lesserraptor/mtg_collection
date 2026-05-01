"""Sets routes: set completion tracker."""

from fastapi import APIRouter, Query, Request

from src.web.routes._set_info import _SET_INFO, _ensure_set_info, MAJOR_SET_TYPES

router = APIRouter()


# Set list: one row per set with completion/wildcard summary.
# Filters:
# - is_rebalanced=0 (no alchemy variants)
# - rarity NOT IN ('basic') (basics are free/unlimited)
# - booster=1 when the set is booster-backed — skips cards that can't be opened
#   from packs (starter-deck exclusives, commander reprints in a crossover set, etc.).
#   "Booster-backed" is defined as "has >= 50 booster=1 cards": real Arena packs
#   always have well over 50 cards in them. Sets whose entire contents are non-booster
#   (Alchemy digital-only sets, commander sets, TMT source material) keep every card
#   because "booster completion" isn't meaningful there.
# - HAVING total_cards >= 50 (removes noise sets; Scryfall enrichment can pull in a
#   handful of cards from paper-only sets like Ravnica Remastered that aren't real
#   Arena releases).
# ORDER BY MAX(arena_id) DESC: highest arena_id = most recently added to MTGA
# (accurate recency proxy).
SET_LIST_SQL = """
    WITH booster_sets AS (
        SELECT set_code FROM cards
        WHERE is_rebalanced = 0 AND rarity NOT IN ('basic') AND booster = 1
        GROUP BY set_code HAVING COUNT(*) >= 50
    )
    SELECT
        c.set_code,
        COUNT(DISTINCT c.arena_id) AS total_cards,
        SUM(CASE WHEN COALESCE(col.quantity, 0) >= 4 THEN 1 ELSE 0 END) AS complete,
        SUM(CASE WHEN COALESCE(col.quantity, 0) = 0 THEN 1 ELSE 0 END) AS missing,
        SUM(CASE WHEN c.rarity = 'mythic' AND COALESCE(col.quantity, 0) < 4
                 THEN 4 - COALESCE(col.quantity, 0) ELSE 0 END) AS mythic_wildcards,
        SUM(CASE WHEN c.rarity = 'rare' AND COALESCE(col.quantity, 0) < 4
                 THEN 4 - COALESCE(col.quantity, 0) ELSE 0 END) AS rare_wildcards,
        SUM(CASE WHEN c.rarity = 'uncommon' AND COALESCE(col.quantity, 0) < 4
                 THEN 4 - COALESCE(col.quantity, 0) ELSE 0 END) AS uncommon_wildcards,
        SUM(CASE WHEN c.rarity = 'common' AND COALESCE(col.quantity, 0) < 4
                 THEN 4 - COALESCE(col.quantity, 0) ELSE 0 END) AS common_wildcards,
        MAX(c.arena_id) AS max_arena_id,
        SUM(CASE WHEN COALESCE(col.quantity, 0) >= 1 THEN 1 ELSE 0 END) AS have_any,
        SUM(MIN(COALESCE(col.quantity, 0), 4)) AS owned_copies,
        SUM(CASE WHEN c.rarity = 'mythic'   AND COALESCE(col.quantity, 0) = 0 THEN 1 ELSE 0 END) AS mythic_missing,
        SUM(CASE WHEN c.rarity = 'rare'     AND COALESCE(col.quantity, 0) = 0 THEN 1 ELSE 0 END) AS rare_missing,
        SUM(CASE WHEN c.rarity = 'uncommon' AND COALESCE(col.quantity, 0) = 0 THEN 1 ELSE 0 END) AS uncommon_missing,
        SUM(CASE WHEN c.rarity = 'common'   AND COALESCE(col.quantity, 0) = 0 THEN 1 ELSE 0 END) AS common_missing
    FROM cards c
    LEFT JOIN collection col ON col.arena_id = c.arena_id
    WHERE c.is_rebalanced = 0 AND c.rarity NOT IN ('basic')
      AND (c.booster = 1 OR c.set_code NOT IN (SELECT set_code FROM booster_sets))
    GROUP BY c.set_code
    HAVING total_cards >= 50
    ORDER BY max_arena_id DESC
"""

# Per-rarity breakdown for a single set.
SET_DETAIL_SQL = """
    SELECT
        c.rarity,
        COUNT(DISTINCT c.arena_id) AS total,
        SUM(CASE WHEN COALESCE(col.quantity, 0) >= 4 THEN 1 ELSE 0 END) AS have_4,
        SUM(CASE WHEN COALESCE(col.quantity, 0) = 0 THEN 1 ELSE 0 END) AS missing,
        SUM(CASE WHEN 4 - COALESCE(col.quantity, 0) > 0
                 THEN 4 - COALESCE(col.quantity, 0) ELSE 0 END) AS wildcards_needed
    FROM cards c
    LEFT JOIN collection col ON col.arena_id = c.arena_id
    WHERE LOWER(c.set_code) = LOWER(?)
      AND c.is_rebalanced = 0
      AND c.rarity NOT IN ('basic')
      AND (? = 0 OR c.booster = 1)
    GROUP BY c.rarity
    ORDER BY CASE c.rarity
        WHEN 'mythic' THEN 1
        WHEN 'rare' THEN 2
        WHEN 'uncommon' THEN 3
        WHEN 'common' THEN 4
        ELSE 5 END
"""

# Missing cards for a set (owned < 4 copies).
MISSING_CARDS_SQL = """
    WITH deck_demand AS (
        SELECT
            LOWER(CASE WHEN instr(dl.card_name, ' // ') > 0
                       THEN substr(dl.card_name, 1, instr(dl.card_name, ' // ') - 1)
                       ELSE dl.card_name END) AS name_lower,
            COUNT(DISTINCT dl.deck_id) AS deck_count
        FROM deck_lines dl
        JOIN decks d ON d.id = dl.deck_id AND (d.is_potential = 1 OR d.is_saved = 1)
        WHERE dl.section = 'mainboard'
        GROUP BY name_lower
    )
    SELECT
        c.arena_id, c.name, c.rarity, c.mana_cost, c.type_line,
        c.image_uri_front, c.colors, c.set_code,
        COALESCE(col.quantity, 0) AS owned_quantity,
        CASE WHEN 4 - COALESCE(col.quantity, 0) > 0
             THEN 4 - COALESCE(col.quantity, 0) ELSE 0 END AS wildcards_needed,
        COALESCE(dd.deck_count, 0) AS deck_demand
    FROM cards c
    LEFT JOIN collection col ON col.arena_id = c.arena_id
    LEFT JOIN deck_demand dd ON dd.name_lower = LOWER(c.name)
    WHERE LOWER(c.set_code) = LOWER(?)
      AND c.is_rebalanced = 0
      AND c.rarity NOT IN ('basic')
      AND (? = 0 OR c.booster = 1)
      AND COALESCE(col.quantity, 0) < 4
    ORDER BY
        CASE c.rarity WHEN 'mythic' THEN 1 WHEN 'rare' THEN 2
                      WHEN 'uncommon' THEN 3 WHEN 'common' THEN 4 ELSE 5 END,
        c.name
"""


def _get_set_summaries(db) -> list[dict]:
    """Return set list with completion and wildcard cost summaries."""
    _ensure_set_info()
    rows = db.execute(SET_LIST_SQL).fetchall()
    result = []
    for r in rows:
        code = r["set_code"].lower()
        info = _SET_INFO.get(code, {})
        d = dict(r)
        d["set_name"] = info.get("name") or code.upper()
        d["icon_svg_uri"] = info.get("icon_svg_uri", "")
        d["set_type"] = info.get("set_type", "")
        result.append(d)
    return result


# Sets with fewer than this many booster=1 cards are treated as "non-booster sets"
# (Alchemy digital-only, commander crossover, etc.): the per-card booster flag is
# ignored and the whole set is shown. Kept in sync with SET_LIST_SQL's booster_sets CTE.
_BOOSTER_SET_THRESHOLD = 50


def _set_is_booster_backed(db, set_code: str) -> bool:
    """Return True when this set has >= _BOOSTER_SET_THRESHOLD booster=1 cards."""
    row = db.execute(
        "SELECT COUNT(*) AS n FROM cards "
        "WHERE LOWER(set_code) = LOWER(?) "
        "  AND is_rebalanced = 0 AND rarity NOT IN ('basic') AND booster = 1",
        (set_code,),
    ).fetchone()
    return (row["n"] if row else 0) >= _BOOSTER_SET_THRESHOLD


def _get_set_breakdown(db, set_code: str) -> dict:
    """Return per-rarity breakdown and missing card list for one set."""
    _ensure_set_info()
    code = set_code.lower()
    info = _SET_INFO.get(code, {})
    booster_filter = 1 if _set_is_booster_backed(db, set_code) else 0
    rarity_rows = db.execute(SET_DETAIL_SQL, (set_code, booster_filter)).fetchall()
    missing_rows = db.execute(MISSING_CARDS_SQL, (set_code, booster_filter)).fetchall()
    return {
        "set_code": set_code,
        "set_name": info.get("name") or set_code.upper(),
        "icon_svg_uri": info.get("icon_svg_uri", ""),
        "by_rarity": [dict(r) for r in rarity_rows],
        "missing_cards": [dict(r) for r in missing_rows],
    }


@router.get("/sets")
async def set_tracker_view(request: Request):
    db = request.app.state.db
    templates = request.app.state.templates
    sets = _get_set_summaries(db)
    ctx = {
        "request": request,
        "mode": "analysis",
        "view": "sets",
        "sets": sets,
        "primary_set_types": sorted(MAJOR_SET_TYPES),
    }
    if request.headers.get("HX-Request"):
        return templates.TemplateResponse(request, "partials/sets_content.html", ctx)
    return templates.TemplateResponse(request, "analysis.html", ctx)


@router.get("/sets/{set_code}/detail")
async def set_detail_partial(
    request: Request,
    set_code: str,
    rarity_filter: list[str] = Query(default=[]),
    owned_filter: str = Query(default=""),
):
    """HTMX-only endpoint: returns the set detail fragment for the modal."""
    db = request.app.state.db
    templates = request.app.state.templates
    breakdown = _get_set_breakdown(db, set_code)
    # Apply rarity filter
    if rarity_filter:
        breakdown["missing_cards"] = [
            c for c in breakdown["missing_cards"]
            if c["rarity"] in rarity_filter
        ]
    # Apply owned filter
    if owned_filter == "missing":
        breakdown["missing_cards"] = [c for c in breakdown["missing_cards"] if c["owned_quantity"] == 0]
    elif owned_filter == "partial":
        breakdown["missing_cards"] = [c for c in breakdown["missing_cards"] if 0 < c["owned_quantity"] < 4]
    # "" (default) = show all cards with owned < 4 (existing behavior)
    ctx = {
        "request": request,
        "mode": "analysis",
        "set_code": set_code,
        "breakdown": breakdown,
        "rarity_filter": rarity_filter,
        "owned_filter": owned_filter,
    }
    return templates.TemplateResponse(request, "partials/set_detail_content.html", ctx)
