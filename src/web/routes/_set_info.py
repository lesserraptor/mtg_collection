"""Shared set info cache: _SET_INFO dict and _ensure_set_info() loader.

Both sets.py and cards.py import from here so they share the same
module-level singleton — no double-fetch from Scryfall.
"""

import requests as _requests

# Canonical major set types — used by both the set tracker and collection facet
# so they always show the same list.
MAJOR_SET_TYPES: frozenset[str] = frozenset({"expansion", "core", "masters", "draft_innovation"})

# Module-level cache: set_code (lowercase) → {name, icon_svg_uri, set_type}
_SET_INFO: dict[str, dict] = {}


def invalidate_set_info_cache() -> None:
    """Clear the cached set info so it will be re-fetched on the next request."""
    _SET_INFO.clear()


def _ensure_set_info() -> None:
    """Fetch set names and icons from Scryfall on first call. No-op after that."""
    if _SET_INFO:
        return
    try:
        resp = _requests.get("https://api.scryfall.com/sets", timeout=5)
        resp.raise_for_status()
        for s in resp.json().get("data", []):
            code = s.get("code", "").lower()
            if code:
                _SET_INFO[code] = {
                    "name": s.get("name", code.upper()),
                    "icon_svg_uri": s.get("icon_svg_uri", ""),
                    "set_type": s.get("set_type", ""),
                }
    except Exception:
        pass  # Offline or API error — fall back to set_code display
