"""seventeen_lands.py — Async client for 17lands card ratings API.

Fetches multiple rating metrics for all cards in a set/format combination.
Results are cached by (set_code, format) — fetch once at draft start, reuse per pick.
"""

import logging
import httpx

logger = logging.getLogger(__name__)

BASE_URL = "https://www.17lands.com"

# Module-level cache: (set_code_upper, format_str) → {mtga_id: {field: value}}
_RATINGS_CACHE: dict[tuple[str, str], dict[int, dict[str, float | None]]] = {}

# Metric field → sample count field + threshold mapping
_FIELD_COUNT_MAP = {
    'ever_drawn_win_rate':       ('ever_drawn_game_count',  500),
    'opening_hand_win_rate':     ('opening_hand_game_count', 500),
    'drawn_improvement_win_rate': ('ever_drawn_game_count', 500),
    'win_rate':                  ('game_count',             500),
    'avg_pick':                  ('pick_count',             200),
    'avg_seen':                  ('seen_count',             200),
}

_ALL_FIELDS = list(_FIELD_COUNT_MAP.keys())


async def fetch_ratings(set_code: str, format_str: str) -> dict[int, dict[str, float | None]]:
    """Return {mtga_id: {field: value | None}} for the given set + format.

    Uses module-level cache — subsequent calls for same set/format return immediately.
    Fields with fewer than the threshold samples return None (not 0).
    """
    cache_key = (set_code.upper(), format_str)
    if cache_key in _RATINGS_CACHE:
        logger.debug("17lands cache hit: %s %s", set_code, format_str)
        return _RATINGS_CACHE[cache_key]

    url = f"{BASE_URL}/card_ratings/data"
    params = {"expansion": set_code.upper(), "format": format_str}
    logger.info("17lands fetch: %s %s", set_code, format_str)

    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
        cards = resp.json()
    except httpx.HTTPStatusError as e:
        logger.warning("17lands HTTP error %s for %s/%s", e.response.status_code, set_code, format_str)
        _RATINGS_CACHE[cache_key] = {}
        return {}
    except Exception:
        logger.exception("17lands fetch failed for %s/%s", set_code, format_str)
        _RATINGS_CACHE[cache_key] = {}
        return {}

    ratings: dict[int, dict[str, float | None]] = {}
    for card in cards:
        mtga_id = card.get("mtga_id")
        if not mtga_id:
            continue
        entry: dict[str, float | None] = {}
        for field, (count_field, threshold) in _FIELD_COUNT_MAP.items():
            count = card.get(count_field) or 0
            entry[field] = card.get(field) if count >= threshold else None
        ratings[int(mtga_id)] = entry

    logger.info(
        "17lands: %d cards with ratings for %s/%s",
        sum(1 for e in ratings.values() if any(v is not None for v in e.values())),
        set_code, format_str,
    )
    _RATINGS_CACHE[cache_key] = ratings
    return ratings


# Keep old name as alias for any callers not yet updated
fetch_gihwr = fetch_ratings
