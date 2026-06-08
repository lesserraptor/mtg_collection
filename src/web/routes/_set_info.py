"""Shared set info cache: _SET_INFO dict and _ensure_set_info() loader.

set_info is populated during Scryfall ingest (ingest.populate_set_info_from_scryfall),
never fetched live from the web layer.
"""

import logging

logger = logging.getLogger(__name__)

# Canonical major set types — used by both the set tracker and collection facet
# so they always show the same list.
MAJOR_SET_TYPES: frozenset[str] = frozenset({"expansion", "core", "masters", "draft_innovation"})

# Module-level cache: set_code (lowercase) → {name, icon_svg_uri, set_type}
_SET_INFO: dict[str, dict] = {}


def invalidate_set_info_cache() -> None:
    """Clear the cached set info so it will be re-fetched on the next request."""
    _SET_INFO.clear()


def _ensure_set_info(db) -> None:
    """Populate _SET_INFO from the DB cache.

    The set_info table is populated during Scryfall ingest (see
    ingest.populate_set_info_from_scryfall), so no live Scryfall call
    is needed here.
    """
    if _SET_INFO:
        return

    try:
        rows = db.execute(
            "SELECT set_code, name, icon_svg_uri, set_type FROM set_info"
        ).fetchall()
        for r in rows:
            _SET_INFO[r["set_code"]] = {
                "name": r["name"],
                "icon_svg_uri": r["icon_svg_uri"],
                "set_type": r["set_type"],
            }
    except Exception:
        pass  # Table may not exist yet on first run
