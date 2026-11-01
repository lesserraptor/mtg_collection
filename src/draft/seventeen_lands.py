"""seventeen_lands.py — Async client for 17lands card ratings API.

Fetches multiple rating metrics for all cards in a set/format combination.
Results are cached by (set_code, format, user_group, time_period) — fetch once at draft start, reuse per pick.
"""

import logging

import httpx

logger = logging.getLogger(__name__)

BASE_URL = "https://www.17lands.com"

# Module-level cache: (set_code_upper, format_str, user_group, time_period) → {mtga_id: {field: value}}
_RATINGS_CACHE: dict[tuple[str, str, str, str], dict[int, dict[str, float | None]]] = {}

# Fields returned by the 17lands API
_API_FIELDS = (
    "ever_drawn_win_rate",
    "opening_hand_win_rate",
    "drawn_improvement_win_rate",
    "win_rate",
    "avg_pick",
    "avg_seen",
)

_EMPTY_API_ENTRY: dict[str, float | None] = {field: None for field in _API_FIELDS}


async def fetch_ratings(
    set_code: str,
    format_str: str,
    user_group: str = "all",
    time_period: str = "ALL_TIME",
    color: str | None = None,
) -> dict[int, dict[str, float | None]]:
    """Return {mtga_id: {field: value | None}} for the given set + format.

    Uses module-level cache — subsequent calls for same params return immediately.
    Fields are percentages (0-100) for win rates, raw values for averages.
    """
    cache_key = (set_code.upper(), format_str, user_group, time_period)
    if cache_key in _RATINGS_CACHE:
        logger.debug("17lands cache hit: %s %s [%s] [%s]", set_code, format_str, user_group, time_period)
        return _RATINGS_CACHE[cache_key]

    url = f"{BASE_URL}/api/card_data"
    params = {
        "expansion": set_code.upper(),
        "event_type": format_str,
        "time_period": time_period,
    }
    if user_group == "top":
        params["user_group"] = "top"
    if color:
        params["color"] = color

    logger.debug("17lands fetch: %s %s [%s] [%s]", set_code, format_str, user_group, time_period)

    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
        raw = resp.json()
        logger.debug("17lands raw response type: %s", type(raw).__name__)
        if isinstance(raw, list):
            logger.debug("17lands raw response length: %d", len(raw))
            if raw:
                logger.debug("17lands sample entry: %r", raw[0])
        elif isinstance(raw, dict):
            logger.debug("17lands raw response keys: %s", list(raw.keys())[:10])
        cards = raw
        if isinstance(cards, dict):
            for key in ("card_data", "data", "cards"):
                if isinstance(cards.get(key), list):
                    cards = cards[key]
                    break
            else:
                # Dict keyed by set code or card name where values are card dicts.
                if all(isinstance(v, dict) for v in cards.values()):
                    cards = list(cards.values())
                else:
                    cards = []
        if not isinstance(cards, list):
            logger.warning("17lands response is not a list of cards: %s", type(cards).__name__)
            cards = []
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
        if not isinstance(card, dict):
            logger.debug("17lands: skipping non-dict card item: %r", card)
            continue
        mtga_id = card.get("mtga_id")
        if not mtga_id:
            continue
        entry: dict[str, float | None] = {}
        for field in _API_FIELDS:
            entry[field] = card.get(field)
        ratings[int(mtga_id)] = entry

    logger.debug(
        "17lands: %d cards with ratings for %s/%s [%s] [%s]",
        sum(1 for e in ratings.values() if any(v is not None for v in e.values())),
        set_code,
        format_str,
        user_group,
        time_period,
    )
    _RATINGS_CACHE[cache_key] = ratings
    return ratings


# Keep old name as alias for any callers not yet updated
fetch_gihwr = fetch_ratings


async def get_ratings(
    set_code: str,
    format_str: str,
    user_group: str = "all",
    time_period: str = "ALL_TIME",
    color: str | None = None,
) -> dict[int, dict[str, float | None]]:
    """Fetch ratings from the 17lands API.

    No local fallback — API is the sole source of truth.
    """
    return await fetch_ratings(set_code, format_str, user_group, time_period, color)