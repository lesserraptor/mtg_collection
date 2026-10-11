"""draft.py — FastAPI router for the Draft Tracker spike.

Routes:
  GET /draft          — HTML page (renders draft.html template)
  GET /draft/stream   — SSE stream of pack_update events
"""

import asyncio
import logging
from collections.abc import AsyncIterable
from pathlib import Path

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.sse import EventSourceResponse, ServerSentEvent

from src.draft.seventeen_lands import fetch_ratings
from src.draft.log_scanner import render_pack_html, start_draft_scanner, METRICS
from src.config import DATA_DIR

logger = logging.getLogger(__name__)
router = APIRouter()

MOCK_LOG_PATH = DATA_DIR / "Player.log"


def _selected_log_path(app) -> Path | None:
    """Return the log the scanner should watch: mock log if mock mode on, else primary."""
    if getattr(app.state, "mock_draft_enabled", False):
        MOCK_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        MOCK_LOG_PATH.touch(exist_ok=True)   # ensure it exists so scanner can watch it
        return MOCK_LOG_PATH
    from src.db.log_parser import find_player_log
    return find_player_log(db=app.state.db)

ALLOWED_FORMATS = ("PremierDraft", "QuickDraft", "TradDraft", "PickTwoDraft")

_MAJOR_SET_TYPES = {"expansion", "core", "masters", "draft_innovation"}


def _get_draft_sets(db) -> list[dict]:
    """Return major draftable sets for the set picker, newest first.

    Uses Scryfall set_type to filter when available; falls back to a card-count
    threshold (>=75 unique cards) if the Scryfall cache is empty.
    """
    from src.web.routes._set_info import _SET_INFO, _ensure_set_info
    _ensure_set_info(db)
    rows = db.execute("""
        SELECT set_code, MAX(arena_id) AS max_arena_id, COUNT(DISTINCT arena_id) AS cnt
        FROM cards
        WHERE is_rebalanced = 0 AND rarity NOT IN ('basic') AND is_active = 1
        GROUP BY set_code
        HAVING cnt >= 10
        ORDER BY max_arena_id DESC
    """).fetchall()
    sets = []
    for row in rows:
        code = row["set_code"].lower()
        info = _SET_INFO.get(code, {})
        set_type = info.get("set_type", "")
        if _SET_INFO:
            # Scryfall data available — filter to draftable set types only
            if set_type not in _MAJOR_SET_TYPES:
                continue
        else:
            # Scryfall unavailable — use card count as proxy for a real set
            if row["cnt"] < 75:
                continue
        sets.append({
            "code": row["set_code"],
            "name": info.get("name") or row["set_code"].upper(),
        })
    return sets


@router.get("/draft", response_class=HTMLResponse)
async def draft_page(request: Request):
    """Render the draft tracker page."""
    templates = request.app.state.templates
    draft_state = getattr(request.app.state, "draft_state", None)
    metric_order = getattr(request.app.state, 'draft_metric_order', ['gihwr', 'ohwr', 'iwd', 'gwr', 'ata', 'alsa'])
    metric = getattr(request.app.state, 'draft_metric', 'gihwr')
    fmt_override = getattr(request.app.state, 'draft_format_override', None)
    detected_format = draft_state.format if draft_state else ""
    draft_format = fmt_override or detected_format or "PremierDraft"
    set_override = getattr(request.app.state, 'draft_set_override', None)
    context = {
        "active_tab": "draft",
        "mode": "draft",
        "phase": draft_state.phase.name if draft_state else "IDLE",
        "set_code": draft_state.set_code if draft_state else "",
        "draft_scanning": getattr(request.app.state, "draft_scanning", False),
        "color_filter": getattr(request.app.state, "draft_color_filter", set()),
        "mock_draft": getattr(request.app.state, "mock_draft_enabled", False),
        "metric": metric,
        "metric_order": metric_order,
        "METRICS": METRICS,
        "draft_format": draft_format,
        "detected_format": detected_format,
        "ALLOWED_FORMATS": ALLOWED_FORMATS,
        "detected_set": draft_state.set_code if draft_state else "",
        "set_override": set_override or "",
        "draft_sets": _get_draft_sets(request.app.state.db),
    }
    return templates.TemplateResponse(request, "draft.html", context)


@router.get("/draft/stream", response_class=EventSourceResponse)
async def draft_stream(request: Request) -> AsyncIterable[ServerSentEvent]:
    """SSE endpoint — streams pack_update events to the browser.

    The browser connects once; events are pushed whenever DraftState changes.
    Client disconnect exits the generator cleanly.
    """
    # Ensure SSE queue exists (created in app startup)
    queue: asyncio.Queue = getattr(request.app.state, "draft_event_queue", None)
    if queue is None:
        # Draft scanner not started (no log file) — stream idle events
        logger.warning("draft_stream: draft_event_queue not initialized")
        while True:
            if await request.is_disconnected():
                break
            await asyncio.sleep(15.0)
        return

    while True:
        if await request.is_disconnected():
            logger.debug("draft_stream: client disconnected")
            break
        try:
            html_string = await asyncio.wait_for(queue.get(), timeout=15.0)
            yield ServerSentEvent(raw_data=html_string, event="pack_update")
        except asyncio.TimeoutError:
            continue  # FastAPI sends keep-alive; this is extra safety


@router.post("/draft/fetch-ratings")
async def trigger_ratings_fetch(request: Request, set_code: str, format: str = "PremierDraft"):
    """Dev/test endpoint: manually trigger a 17lands fetch and store ratings on draft_state."""
    draft_state = getattr(request.app.state, "draft_state", None)
    if draft_state is None:
        return {"error": "no draft state"}
    ratings = await fetch_ratings(set_code, format)
    draft_state.ratings = ratings
    return {"set_code": set_code, "format": format, "cards_with_ratings": sum(1 for e in ratings.values() if any(v is not None for v in e.values()))}


@router.post("/draft/scan/start")
async def scan_start(request: Request):
    """Start the log file watcher — user-initiated, not automatic."""
    if getattr(request.app.state, "draft_scanning", False):
        return RedirectResponse("/draft", status_code=303)
    log_path = _selected_log_path(request.app)
    if not log_path or not log_path.exists():
        templates = request.app.state.templates
        return templates.TemplateResponse(
            request, "draft.html",
            {
                "active_tab": "draft",
                "mode": "draft",
                "phase": request.app.state.draft_state.phase.name,
                "set_code": request.app.state.draft_state.set_code,
                "draft_scanning": False,
                "scan_error": f"Log file not found: {log_path or 'no path configured'}",
                "color_filter": getattr(request.app.state, "draft_color_filter", set()),
                "metric": getattr(request.app.state, "draft_metric", "gihwr"),
                "metric_order": getattr(request.app.state, "draft_metric_order", ["gihwr", "ohwr", "iwd", "gwr", "ata", "alsa"]),
                "METRICS": METRICS,
                "mock_draft": getattr(request.app.state, "mock_draft_enabled", False),
            },
            status_code=200,
        )
    from src.draft.state import DraftState
    state = DraftState()
    request.app.state.draft_state = state
    await start_draft_scanner(request.app, log_path, request.app.state.draft_state)
    request.app.state.draft_scanning = True
    request.app.state.draft_format_override = None
    request.app.state.draft_set_override = None
    logger.info("draft scanner: started by user — watching %s", log_path)
    return RedirectResponse("/draft", status_code=303)


@router.post("/draft/scan/stop")
async def scan_stop(request: Request):
    """Stop the log file watcher and reset draft state."""
    import asyncio
    from src.draft.state import DraftState
    observer = getattr(request.app.state, "draft_observer", None)
    if observer:
        observer.unschedule_all()
        observer.stop()
        await asyncio.get_event_loop().run_in_executor(None, observer.join)
        request.app.state.draft_observer = None
    request.app.state.draft_scanning = False
    request.app.state.draft_state = DraftState()
    request.app.state.draft_set_override = None
    logger.info("draft scanner: stopped by user")
    return RedirectResponse("/draft", status_code=303)


@router.post("/draft/mock/toggle")
async def toggle_mock_draft(request: Request):
    """Toggle mock-draft mode and restart the scanner on the chosen log if running."""
    app = request.app
    cur = getattr(app.state, "mock_draft_enabled", False)
    app.state.mock_draft_enabled = not cur
    app.state.db.execute(
        "INSERT OR REPLACE INTO meta (key,value) VALUES ('mock_draft',?)",
        ("1" if app.state.mock_draft_enabled else "0"),
    )
    app.state.db.commit()
    # If a scanner is running, stop it and restart on the newly-selected log so the change is live.
    if getattr(app.state, "draft_scanning", False):
        # ---- stop (mirror scan_stop) ----
        import asyncio
        observer = getattr(app.state, "draft_observer", None)
        if observer:
            observer.unschedule_all()
            observer.stop()
            await asyncio.get_event_loop().run_in_executor(None, observer.join)
            app.state.draft_observer = None
        app.state.draft_scanning = False
        # ---- start on new log ----
        from src.draft.state import DraftState
        app.state.draft_state = DraftState()
        log_path = _selected_log_path(app)
        if log_path:
            await start_draft_scanner(app, log_path, app.state.draft_state)
            app.state.draft_scanning = True
    templates = app.state.templates
    return templates.TemplateResponse(
        request, "partials/draft_sidebar.html", _sidebar_context(request)
    )


def _sidebar_context(request: Request) -> dict:
    """Build context dict for the draft_sidebar partial."""
    metric_order = getattr(request.app.state, 'draft_metric_order', ['gihwr', 'ohwr', 'iwd', 'gwr', 'ata', 'alsa'])
    metric = getattr(request.app.state, 'draft_metric', 'gihwr')
    draft_state = getattr(request.app.state, 'draft_state', None)
    fmt_override = getattr(request.app.state, 'draft_format_override', None)
    detected_format = draft_state.format if draft_state else ""
    draft_format = fmt_override or detected_format or "PremierDraft"
    set_override = getattr(request.app.state, 'draft_set_override', None)
    return {
        "color_filter": getattr(request.app.state, 'draft_color_filter', set()),
        "mock_draft": getattr(request.app.state, "mock_draft_enabled", False),
        "metric": metric,
        "metric_order": metric_order,
        "METRICS": METRICS,
        "draft_scanning": getattr(request.app.state, 'draft_scanning', False),
        "scan_error": None,
        "draft_format": draft_format,
        "detected_format": detected_format,
        "ALLOWED_FORMATS": ALLOWED_FORMATS,
        "detected_set": draft_state.set_code if draft_state else "",
        "set_override": set_override or "",
        "draft_sets": _get_draft_sets(request.app.state.db),
    }


async def _rerender_pack(request: Request):
    """Push re-rendered pack HTML to SSE queue, using format/set overrides if set."""
    queue = getattr(request.app.state, "draft_event_queue", None)
    if queue:
        state = getattr(request.app.state, "draft_state", None)
        if state:
            fmt_override = getattr(request.app.state, "draft_format_override", None)
            set_override = getattr(request.app.state, "draft_set_override", None)
            active_fmt = fmt_override or state.format or "PremierDraft"
            active_set = set_override or state.set_code
            if (fmt_override or set_override) and active_set:
                active_ratings = await fetch_ratings(active_set, active_fmt)
                # Sync state so log_scanner doesn't re-fetch on next pack
                state.ratings = active_ratings
                state.last_fetched_format = active_fmt
            else:
                active_ratings = state.ratings
            queue.put_nowait(render_pack_html(request.app, state, active_fmt, active_ratings, active_set))


@router.post("/draft/metric/set")
async def set_draft_metric(request: Request, metric: str = Form("")):
    """Set the active ranking metric and re-render the current pack."""
    if metric not in METRICS:
        return HTMLResponse("", status_code=400)
    request.app.state.draft_metric = metric
    # Persist to meta
    request.app.state.db.execute(
        "INSERT OR REPLACE INTO meta (key,value) VALUES ('draft_metric',?)", (metric,)
    )
    request.app.state.db.commit()
    await _rerender_pack(request)
    templates = request.app.state.templates
    return templates.TemplateResponse(
        request, "partials/draft_sidebar.html", _sidebar_context(request)
    )


@router.post("/draft/colors/toggle")
async def toggle_draft_color(request: Request, color: str = Form("")):
    """Toggle a color in the draft color filter and re-render the current pack."""
    color = color.upper()
    if color not in ("W", "U", "B", "R", "G"):
        return HTMLResponse("", status_code=400)
    cf = getattr(request.app.state, 'draft_color_filter', set())
    if color in cf:
        cf.discard(color)
    else:
        cf.add(color)
    request.app.state.draft_color_filter = cf
    await _rerender_pack(request)
    templates = request.app.state.templates
    return templates.TemplateResponse(
        request, "partials/draft_sidebar.html", _sidebar_context(request)
    )


@router.post("/draft/colors/clear")
async def clear_draft_colors(request: Request):
    """Clear the draft color filter."""
    request.app.state.draft_color_filter = set()
    await _rerender_pack(request)
    templates = request.app.state.templates
    return templates.TemplateResponse(
        request, "partials/draft_sidebar.html", _sidebar_context(request)
    )


@router.post("/draft/format/set")
async def set_draft_format(request: Request, format: str = Form("")):
    """Set the user's format picker selection and re-render the current pack."""
    if format not in ALLOWED_FORMATS:
        return HTMLResponse("", status_code=400)
    request.app.state.draft_format_override = format
    await _rerender_pack(request)
    templates = request.app.state.templates
    return templates.TemplateResponse(
        request, "partials/draft_sidebar.html", _sidebar_context(request)
    )


@router.post("/draft/set/set")
async def set_draft_set_override(request: Request, set_code: str = Form("")):
    """Override the detected set code for 17lands lookups and re-render."""
    set_code = set_code.strip().upper()
    request.app.state.draft_set_override = set_code or None
    # Clear cached ratings so re-fetch uses the new set code
    draft_state = getattr(request.app.state, "draft_state", None)
    if draft_state:
        draft_state.ratings = {}
    await _rerender_pack(request)
    templates = request.app.state.templates
    return templates.TemplateResponse(
        request, "partials/draft_sidebar.html", _sidebar_context(request)
    )
