"""FastAPI application entry point for the MTGA Collection Browser."""

import asyncio
import logging
import re

from fastapi import FastAPI

logger = logging.getLogger(__name__)
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import RedirectResponse
from markupsafe import Markup

from src.db.schema import init_db
from src.web.routes import images as images_router
from src.web.routes import cards as cards_router
from src.web.routes import decks as decks_router
from src.web.routes import analysis as analysis_router
from src.web.routes import settings as settings_router
from src.web.routes import changes as changes_router
from src.web.routes import sets as sets_router
from src.web.routes import draft as draft_router

app = FastAPI(title="MTGA Collection Browser")

# Static files
app.mount("/static", StaticFiles(directory="src/web/static"), name="static")

# Templates — shared across routers via app.state
templates = Jinja2Templates(directory="src/web/templates")


def _filter_query_string(request, exclude_keys=None):
    """Rebuild query string from current request, excluding specified keys.
    Used in templates to build pagination links that preserve filter state.
    """
    from urllib.parse import urlencode
    exclude = set(exclude_keys or ["page"])
    params = [(k, v) for k, v in request.query_params.multi_items()
              if k not in exclude]
    return urlencode(params)


templates.env.globals["filter_query_string"] = _filter_query_string


def _parse_mana_cost(mana_cost: str) -> list[str]:
    """Parse a mana cost string like '{2}{W}{U}' into a list of symbols ['2','W','U'].

    Handles DFC costs like '{2}{B} // {3}{B}{B}' by inserting a '//' sentinel.
    Handles hybrid costs like '{(U/R)}' by stripping parens and slashes.
    """
    if not mana_cost:
        return []
    if " // " in mana_cost:
        left, right = mana_cost.split(" // ", 1)
        return _parse_mana_cost(left) + ["//"] + _parse_mana_cost(right)
    cleaned = [s.strip().replace("/", "").replace("(", "").replace(")", "") for s in mana_cost.replace("{", "").split("}") if s.strip()]
    return [t for t in cleaned if t]


_NUMERIC_MANA = set("0123456789XYZS")

# Single-color mana CSS class map (matches app.css .mana-* classes)
_MANA_CSS_CLASS: dict[str, str] = {
    "W": "mana-W", "U": "mana-U", "B": "mana-B", "R": "mana-R", "G": "mana-G",
}

# Two-color gradient colors for hybrid symbols (matches app.css .mana-* backgrounds)
_MANA_COLORS: dict[str, str] = {
    "W": "#f9f7e8", "U": "#c8daf4", "B": "#555", "R": "#f4a57a", "G": "#8fcb8a", "C": "#d0c6bb",
}


def _hybrid_gradient(sym_upper: str) -> str | None:
    """Return a CSS gradient string for a two-color hybrid symbol, or None."""
    if len(sym_upper) != 2:
        return None
    c1 = _MANA_COLORS.get(sym_upper[0])
    c2 = _MANA_COLORS.get(sym_upper[1])
    if c1 and c2:
        return f"linear-gradient(135deg,{c1} 50%,{c2} 50%)"
    return None


def _mana_symbols_html(mana_cost: str, small: bool = False) -> Markup:
    """Render a mana cost string as pure-CSS mana pip spans.

    Uses the .mana/.mana-* CSS classes from app.css — fixed px sizing, no
    icon font, so rendering is identical in all browsers.
    """
    symbols = _parse_mana_cost(mana_cost)
    size_style = "width:11px;height:11px;font-size:6px;" if small else ""
    parts = []
    for sym in symbols:
        sym_upper = sym.upper()
        if not sym_upper:
            continue
        if sym_upper == "//":
            parts.append('<span style="font-size:0.85em;color:#888;padding:0 1px;">//</span>')
            continue
        grad = _hybrid_gradient(sym_upper)
        if grad:
            # Hybrid: CSS gradient circle with two-letter label
            label_style = "font-family:system-ui,sans-serif;font-size:0.55em;font-weight:800;line-height:1;color:#333;letter-spacing:-1px;"
            parts.append(
                f'<span class="mana" style="background:{grad};{size_style}">'
                f'<span style="{label_style}">{sym_upper[0]}/{sym_upper[1]}</span>'
                f'</span>'
            )
        elif sym_upper in _MANA_CSS_CLASS:
            # Single-color: .mana circle (fixed px, no font metrics) wrapping mana font icon
            icon_sz = "7px" if small else "10px"
            css_class = _MANA_CSS_CLASS[sym_upper]
            parts.append(
                f'<span class="mana {css_class}" style="{size_style}">'
                f'<i class="ms ms-{sym.lower()}" style="font-size:{icon_sz};line-height:1;"></i>'
                f'</span>'
            )
        else:
            # Generic/numeric/colorless: letter or number in grey circle
            css_class = _MANA_CSS_CLASS.get(sym_upper, "mana-generic")
            parts.append(
                f'<span class="mana {css_class}" style="{size_style}">{sym_upper}</span>'
            )
    return Markup("".join(parts))


templates.env.globals["parse_mana_cost"] = _parse_mana_cost
templates.env.globals["mana_symbols_html"] = _mana_symbols_html


def _oracle_to_html(text: str) -> Markup:
    """Convert oracle text with {SYMBOL} tokens to mana-font icon HTML.

    Also converts newlines to <br> tags.
    """
    if not text:
        return Markup("")

    _CSS_NAME = {"t": "tap", "q": "untap"}

    def replace_symbol(m: re.Match) -> str:
        sym = m.group(1).lower().replace("/", "").replace("(", "").replace(")", "")
        css = _CSS_NAME.get(sym, sym)
        return f'<i class="ms ms-{css} ms-cost"></i>'

    result = re.sub(r"\{([^}]+)\}", replace_symbol, text)
    result = result.replace("\n", "<br>")
    return Markup(result)


templates.env.globals["oracle_to_html"] = _oracle_to_html


@app.on_event("startup")
async def startup():
    import os
    from pathlib import Path

    app.state.db = init_db()
    app.state.templates = templates
    from src.collection import find_collection_file
    from src.watcher import start_watcher
    collection_path = find_collection_file()
    if collection_path:
        await start_watcher(app, collection_path)

    # Initialize draft tracker state
    from src.draft.state import DraftState

    app.state.draft_state = DraftState()
    app.state.draft_scanning = False
    app.state.draft_color_filter = set()
    app.state.draft_event_queue = asyncio.Queue()

    import json as _json
    _draft_metric = app.state.db.execute(
        "SELECT value FROM meta WHERE key='draft_metric'"
    ).fetchone()
    app.state.draft_metric = _draft_metric[0] if _draft_metric else 'gihwr'

    _metric_order_row = app.state.db.execute(
        "SELECT value FROM meta WHERE key='draft_metric_order'"
    ).fetchone()
    app.state.draft_metric_order = (
        _json.loads(_metric_order_row[0]) if _metric_order_row
        else ['gihwr', 'ohwr', 'iwd', 'gwr', 'ata', 'alsa']
    )

    logger.info("draft scanner: not started — use the Start Listening button on /draft")


@app.on_event("shutdown")
async def shutdown():
    if hasattr(app.state, "watcher_observer"):
        app.state.watcher_observer.stop()
        app.state.watcher_observer.join()
    if hasattr(app.state, "draft_observer"):
        app.state.draft_observer.stop()
        app.state.draft_observer.join()
    app.state.db.close()


@app.get("/")
async def root():
    return RedirectResponse(url="/cards")


# Register routers
app.include_router(images_router.router)
app.include_router(cards_router.router)
app.include_router(decks_router.router)
app.include_router(analysis_router.router)
app.include_router(settings_router.router)
app.include_router(changes_router.router)
app.include_router(sets_router.router)
app.include_router(draft_router.router)
