"""Changes routes: recent collection diff feed."""

import math

from fastapi import APIRouter, Query, Request
from fastapi.responses import HTMLResponse

router = APIRouter()

SESSIONS_PER_PAGE = 5

# Count distinct sessions with gains.
SESSIONS_COUNT_QUERY = """
    SELECT COUNT(DISTINCT cs.id)
    FROM collection_snapshot_diffs csd
    JOIN collection_snapshots cs ON cs.id = csd.snapshot_id
    WHERE csd.diff > 0
"""

# Snapshot IDs for one page, newest first.
SESSIONS_PAGE_QUERY = """
    SELECT DISTINCT cs.id
    FROM collection_snapshot_diffs csd
    JOIN collection_snapshots cs ON cs.id = csd.snapshot_id
    WHERE csd.diff > 0
    ORDER BY cs.snapshot_at DESC
    LIMIT ? OFFSET ?
"""

# All card rows for a set of snapshot IDs.
# card_name in collection_snapshot_diffs may be NULL (_persist_diff stores None);
# fall back to cards.name via LEFT JOIN.
FEED_QUERY = """
    SELECT
        cs.id            AS snapshot_id,
        cs.snapshot_at,
        cs.source,
        csd.arena_id,
        COALESCE(c.name, csd.card_name) AS card_name,
        c.rarity,
        c.set_code,
        c.mana_cost,
        c.type_line,
        c.image_uri_front,
        csd.old_quantity,
        csd.new_quantity,
        csd.diff
    FROM collection_snapshot_diffs csd
    JOIN collection_snapshots cs ON cs.id = csd.snapshot_id
    LEFT JOIN cards c ON c.arena_id = csd.arena_id
    WHERE csd.diff > 0 AND cs.id IN ({placeholders})
    ORDER BY cs.snapshot_at DESC,
        CASE c.rarity
            WHEN 'mythic' THEN 1
            WHEN 'rare' THEN 2
            WHEN 'uncommon' THEN 3
            WHEN 'common' THEN 4
            ELSE 5
        END,
        COALESCE(c.name, csd.card_name) ASC
"""


def _fetch_page(db, page: int):
    """Return (sessions, total_sessions, total_pages) for the requested page."""
    total = db.execute(SESSIONS_COUNT_QUERY).fetchone()[0]
    total_pages = max(1, math.ceil(total / SESSIONS_PER_PAGE))
    page = max(1, min(page, total_pages))
    offset = (page - 1) * SESSIONS_PER_PAGE

    snap_ids = [r[0] for r in db.execute(SESSIONS_PAGE_QUERY, (SESSIONS_PER_PAGE, offset)).fetchall()]
    if not snap_ids:
        return [], total, total_pages

    placeholders = ",".join("?" * len(snap_ids))
    rows = db.execute(FEED_QUERY.format(placeholders=placeholders), snap_ids).fetchall()

    groups: dict[int, dict] = {}
    order: list[int] = []
    for row in rows:
        sid = row["snapshot_id"]
        if sid not in groups:
            groups[sid] = {
                "snapshot_id": sid,
                "snapshot_at": row["snapshot_at"],
                "source": row["source"],
                "cards": [],
            }
            order.append(sid)
        groups[sid]["cards"].append(dict(row))

    return [groups[sid] for sid in order], total, total_pages


@router.get("/changes")
async def changes_view(request: Request, page: int = Query(1, ge=1)):
    db = request.app.state.db
    templates = request.app.state.templates
    sessions, total, total_pages = _fetch_page(db, page)

    ctx = {
        "mode": "changes",
        "sessions": sessions,
        "page": page,
        "total": total,
        "total_pages": total_pages,
        "sessions_per_page": SESSIONS_PER_PAGE,
    }

    # HTMX partial swap: return only the swappable content div
    if request.headers.get("HX-Request"):
        return templates.TemplateResponse(request, "partials/changes_content.html", ctx)

    return templates.TemplateResponse(request, "changes.html", ctx)
