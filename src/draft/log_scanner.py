"""log_scanner.py — Watchdog + asyncio bridge for MTGA draft log parsing.

Architecture mirrors src/watcher.py exactly:
  - LogScanner (FileSystemEventHandler) runs in watchdog OS thread.
  - on_modified() only calls loop.call_soon_threadsafe(queue.put_nowait, path).
  - log_consumer() is an asyncio coroutine: dequeues events, reads new bytes,
    calls _process_line() on each new line, updates DraftState, pushes SSE events.
  - DraftState mutation only ever happens on the asyncio thread — safe, no
    cross-thread state access.
"""

import asyncio
import json
import logging
from pathlib import Path

from watchdog.events import FileSystemEventHandler
from watchdog.observers.polling import PollingObserver as Observer

from src.draft.seventeen_lands import fetch_ratings
from src.draft.state import DraftPhase, DraftState

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Metric definitions — importable by routes and templates
# ---------------------------------------------------------------------------
METRICS = {
    'gihwr': {'field': 'ever_drawn_win_rate',       'label': 'GIHWR', 'pct': True,  'higher_better': True},
    'ohwr':  {'field': 'opening_hand_win_rate',      'label': 'OHWR',  'pct': True,  'higher_better': True},
    'iwd':   {'field': 'drawn_improvement_win_rate', 'label': 'IWD',   'pct': True,  'higher_better': True},
    'gwr':   {'field': 'win_rate',                   'label': 'GWR',   'pct': True,  'higher_better': True},
    'ata':   {'field': 'avg_pick',                   'label': 'ATA',   'pct': False, 'higher_better': False},
    'alsa':  {'field': 'avg_seen',                   'label': 'ALSA',  'pct': False, 'higher_better': False},
}
DEFAULT_METRIC_ORDER = ['gihwr', 'ohwr', 'iwd', 'gwr', 'ata', 'alsa']

# ---------------------------------------------------------------------------
# Draft start event prefixes
# ---------------------------------------------------------------------------
DRAFT_START_PREFIXES = [
    "[UnityCrossThreadLogger]==> Event_Join ",
    "[UnityCrossThreadLogger]==> BotDraft_DraftStatus ",
]

# ---------------------------------------------------------------------------
# Pack event prefixes
# ---------------------------------------------------------------------------
PACK_NOTIFY_PREFIX = "[UnityCrossThreadLogger]Draft.Notify "

# ---------------------------------------------------------------------------
# Pick event prefixes
# ---------------------------------------------------------------------------
PICK_PREFIX_V1 = "[UnityCrossThreadLogger]==> Event_PlayerDraftMakePick "
PICK_PREFIX_V2 = "[UnityCrossThreadLogger]==> Draft.MakeHumanDraftPick "
PICK_PREFIX_BOT = "[UnityCrossThreadLogger]==> BotDraft_DraftPick "

# Known Alchemy/digital set code prefixes to strip before querying 17lands
_ALCHEMY_PREFIXES = ("Y25", "Y", "A")

# Draft type → format string mapping
_DRAFT_FORMAT_MAP = {
    "Trad": "TradDraft",
    "TradDraft": "TradDraft",
    "PremierDraft": "PremierDraft",
    "Draft": "PremierDraft",
    "BotDraft": "QuickDraft",
    "QuickDraft": "QuickDraft",
    "Sealed": "Sealed",
    "TradSealed": "TradSealed",
}


def _strip_alchemy_prefix(code: str) -> str:
    """Remove known Alchemy prefixes (Y25, Y, A) from set code."""
    for prefix in _ALCHEMY_PREFIXES:
        if code.startswith(prefix) and len(code) > len(prefix):
            stripped = code[len(prefix):]
            # Only strip if result looks like a real set code (2–4 alpha chars)
            if stripped.isalpha() and 2 <= len(stripped) <= 4:
                return stripped.upper()
    return code.upper()


def _process_line(line: str, state: DraftState) -> bool:
    """Parse a single log line and update state in-place.

    Returns True if state changed (i.e., a draft event was detected).
    All JSON parsing is wrapped in try/except — never raises.
    """
    changed = False

    # ------------------------------------------------------------------
    # Draft start detection
    # ------------------------------------------------------------------
    for prefix in DRAFT_START_PREFIXES:
        idx = line.find(prefix)
        if idx != -1:
            try:
                outer = json.loads(line[idx + len(prefix):])
                request_str = outer.get("request") or outer.get("Request", "{}")
                request = json.loads(request_str)
                payload = request.get("Payload", "")
                if isinstance(payload, str):
                    payload_obj = json.loads(payload)
                else:
                    payload_obj = payload
                event_name = payload_obj.get("EventName", "")
                if event_name:
                    # EventName format: "Trad_Draft_MKM_20240301" or "Draft_MKM_20240301"
                    parts = event_name.split("_")
                    # Determine set code: look for 3-4 char alpha segment
                    set_code_raw = ""
                    draft_type = parts[0] if parts else ""
                    for i, part in enumerate(parts):
                        if part.isalpha() and 2 <= len(part) <= 5 and i > 0:
                            set_code_raw = part
                            break
                    if not set_code_raw and len(parts) >= 3:
                        set_code_raw = parts[2]
                    set_code = _strip_alchemy_prefix(set_code_raw) if set_code_raw else ""
                    # Map draft type to format string
                    # "Trad_Draft" → prefix "Trad" maps differently from "BotDraft"
                    if draft_type == "Trad" and len(parts) >= 2 and parts[1] == "Draft":
                        fmt = "TradDraft"
                    elif draft_type == "BotDraft" or "BotDraft" in prefix or "BotDraft" in parts:
                        fmt = "QuickDraft"
                    else:
                        fmt = _DRAFT_FORMAT_MAP.get(draft_type, "PremierDraft")
                    state.set_code = set_code
                    state.format = fmt
                    state.phase = DraftPhase.DRAFT_STARTED
                    state.pack_cards = []
                    state.taken_cards = []
                    state.ratings = {}
                    state.pack_num = 0
                    state.pick_num = 0
                    changed = True
                    logger.debug(
                        "log_scanner: draft started — set=%s format=%s",
                        state.set_code,
                        state.format,
                    )
            except (json.JSONDecodeError, KeyError, TypeError, ValueError):
                logger.debug("log_scanner: failed to parse draft start line", exc_info=False)
            break  # Only one prefix can match per line

    if changed:
        return changed

    # ------------------------------------------------------------------
    # Pack detection — Shape B (Draft.Notify, post-P1P1)
    # Must be checked before Shape A to avoid double-matching on some lines
    # ------------------------------------------------------------------
    idx = line.find(PACK_NOTIFY_PREFIX)
    if idx != -1:
        try:
            data = json.loads(line[idx + len(PACK_NOTIFY_PREFIX):])
            card_ids = [int(x) for x in data["PackCards"].split(",") if x.strip()]
            state.pack_cards = card_ids
            state.pack_num = data.get("SelfPack", state.pack_num)
            state.pick_num = data.get("SelfPick", state.pick_num)
            state.phase = DraftPhase.PACK_OFFERED
            changed = True
            logger.debug(
                "log_scanner: pack offered (Draft.Notify) — %d cards, P%dP%d",
                len(card_ids),
                state.pack_num,
                state.pick_num,
            )
        except (json.JSONDecodeError, KeyError, TypeError, ValueError):
            logger.debug("log_scanner: failed to parse Draft.Notify line", exc_info=False)
        return changed

    # ------------------------------------------------------------------
    # Pack detection — Shape A (P1P1 CardsInPack)
    # ------------------------------------------------------------------
    if "CardsInPack" in line and "BotDraft_DraftStatus" not in line and "LogBusinessEvents" not in line:
        try:
            json_start = line.index("{")
            data = json.loads(line[json_start:])
            card_ids = [int(x) for x in data.get("CardsInPack", [])]
            if card_ids:
                state.pack_cards = card_ids
                state.pack_num = data.get("PackNumber", 1)
                state.pick_num = data.get("PickNumber", 1)
                state.phase = DraftPhase.PACK_OFFERED
                changed = True
                logger.debug(
                    "log_scanner: pack offered (CardsInPack) — %d cards",
                    len(card_ids),
                )
        except (json.JSONDecodeError, KeyError, TypeError, ValueError):
            logger.debug("log_scanner: failed to parse CardsInPack line", exc_info=False)
        return changed

    # ------------------------------------------------------------------
    # Pack detection — Shape C (Quick Draft DraftPack)
    # ------------------------------------------------------------------
    if 'DraftPack' in line and 'DraftStatus' in line:
        try:
            json_start = line.index("{")
            outer = json.loads(line[json_start:])
            # Real Quick Draft lines: {"CurrentModule":"BotDraft","Payload":"{...}"}
            # Payload is a nested JSON string containing DraftStatus/DraftPack/EventName
            payload_str = outer.get("Payload")
            if isinstance(payload_str, str):
                data = json.loads(payload_str)
            else:
                data = outer
            if data.get("DraftStatus") == "PickNext":
                card_ids = [int(x) for x in data.get("DraftPack", [])]
                if card_ids:
                    state.pack_cards = card_ids
                    state.pack_num = data.get("PackNumber", state.pack_num)
                    state.pick_num = data.get("PickNumber", state.pick_num)
                    state.phase = DraftPhase.PACK_OFFERED
                    changed = True
                    # Merge PickedCards — fixes stale taken_cards after app restart mid-draft
                    picked_ids = [int(x) for x in data.get("PickedCards", []) if str(x).strip()]
                    if len(picked_ids) > len(state.taken_cards):
                        state.taken_cards = picked_ids
                    # Extract set code + format from EventName if not already known
                    if not state.set_code:
                        event_name = data.get("EventName", "")
                        if event_name:
                            parts = event_name.split("_")
                            # Skip parts[0] — it may be an event-wrapper prefix (e.g. "MWM"),
                            # not the draft type or set code.
                            set_code_raw = next(
                                (p for i, p in enumerate(parts) if i > 0 and p.isalpha() and 2 <= len(p) <= 5),
                                parts[1] if len(parts) >= 2 else ""
                            )
                            state.set_code = _strip_alchemy_prefix(set_code_raw) if set_code_raw else ""
                            draft_type = parts[0] if parts else ""
                            # "BotDraft" may appear anywhere in parts (e.g. "MWM_TMT_BotDraft_...")
                            if draft_type in ("BotDraft", "QuickDraft", "QuickDraftEmblem") or "BotDraft" in parts:
                                state.format = "QuickDraft"
                            elif draft_type == "Trad":
                                state.format = "TradDraft"
                            else:
                                state.format = _DRAFT_FORMAT_MAP.get(draft_type, "PremierDraft")
                            logger.debug(
                                "log_scanner: draft detected via DraftPack — set=%s format=%s",
                                state.set_code, state.format,
                            )
                    logger.debug(
                        "log_scanner: pack offered (DraftPack) — %d cards",
                        len(card_ids),
                    )
        except (json.JSONDecodeError, KeyError, TypeError, ValueError):
            logger.debug("log_scanner: failed to parse DraftPack line", exc_info=False)
        return changed

    # ------------------------------------------------------------------
    # Pick detection — Premier V1 / Traditional
    # ------------------------------------------------------------------
    idx = line.find(PICK_PREFIX_V1)
    if idx != -1:
        try:
            outer = json.loads(line[idx + len(PICK_PREFIX_V1):])
            request_str = outer.get("request", "{}")
            request = json.loads(request_str) if isinstance(request_str, str) else request_str
            payload_str = request.get("Payload", "{}")
            payload = json.loads(payload_str) if isinstance(payload_str, str) else payload_str
            arena_id = int(payload["GrpId"])
            state.taken_cards.append(arena_id)
            state.phase = DraftPhase.PICK_MADE
            changed = True
            logger.debug("log_scanner: pick made (V1) — arena_id=%d", arena_id)
        except (json.JSONDecodeError, KeyError, TypeError, ValueError):
            logger.debug("log_scanner: failed to parse pick V1 line", exc_info=False)
        return changed

    # ------------------------------------------------------------------
    # Pick detection — Premier V2
    # ------------------------------------------------------------------
    idx = line.find(PICK_PREFIX_V2)
    if idx != -1:
        try:
            data = json.loads(line[idx + len(PICK_PREFIX_V2):])
            arena_id = int(data["cardId"])
            state.taken_cards.append(arena_id)
            state.phase = DraftPhase.PICK_MADE
            changed = True
            logger.debug("log_scanner: pick made (V2) — arena_id=%d", arena_id)
        except (json.JSONDecodeError, KeyError, TypeError, ValueError):
            logger.debug("log_scanner: failed to parse pick V2 line", exc_info=False)
        return changed

    # ------------------------------------------------------------------
    # Pick detection — Quick Draft (BotDraft)
    # ------------------------------------------------------------------
    idx = line.find(PICK_PREFIX_BOT)
    if idx != -1:
        try:
            outer = json.loads(line[idx + len(PICK_PREFIX_BOT):])
            payload_str = outer.get("Payload", "{}")
            payload = json.loads(payload_str) if isinstance(payload_str, str) else payload_str
            pick_info = payload.get("PickInfo", {})
            arena_id = int(pick_info["CardId"])
            state.taken_cards.append(arena_id)
            state.phase = DraftPhase.PICK_MADE
            changed = True
            logger.debug("log_scanner: pick made (BotDraft) — arena_id=%d", arena_id)
        except (json.JSONDecodeError, KeyError, TypeError, ValueError):
            logger.debug("log_scanner: failed to parse BotDraft pick line", exc_info=False)
        return changed

    # ------------------------------------------------------------------
    # Pack+pick combined — Shape D (LogBusinessEvents auto-pick / timer-expired)
    # ------------------------------------------------------------------
    if "LogBusinessEvents" in line and "PickGrpId" in line:
        try:
            json_start = line.index("{")
            data = json.loads(line[json_start:])
            if "PickGrpId" in data and "CardsInPack" in data:
                card_ids = [int(x) for x in data.get("CardsInPack", [])]
                picked_id = int(data["PickGrpId"])
                if card_ids:
                    state.pack_cards = card_ids
                    state.pack_num = data.get("PackNumber", state.pack_num)
                    state.pick_num = data.get("PickNumber", state.pick_num)
                state.taken_cards.append(picked_id)
                state.phase = DraftPhase.PICK_MADE
                changed = True
                logger.debug(
                    "log_scanner: Shape D auto-pick — arena_id=%d P%sP%s",
                    picked_id, state.pack_num, state.pick_num,
                )
        except (json.JSONDecodeError, KeyError, TypeError, ValueError):
            logger.debug("log_scanner: failed to parse Shape D (LogBusinessEvents) line", exc_info=False)
        return changed

    # ------------------------------------------------------------------
    # Draft complete
    # ------------------------------------------------------------------
    if "Draft_CompleteDraft" in line and "DraftId" in line:
        try:
            json_start = line.index("{")
            data = json.loads(line[json_start:])
            if "DraftId" in data:
                state.phase = DraftPhase.DRAFT_COMPLETE
                state.pack_cards = []
                changed = True
                logger.debug(
                    "log_scanner: draft complete — DraftId=%s", data.get("DraftId")
                )
        except (json.JSONDecodeError, KeyError, TypeError, ValueError):
            logger.debug("log_scanner: failed to parse Draft_CompleteDraft line", exc_info=False)
        return changed

    return changed


# ---------------------------------------------------------------------------
# Server-side rendering helper
# ---------------------------------------------------------------------------

def render_pack_html(app, state: DraftState, active_fmt: str | None = None, active_ratings: dict | None = None, active_set: str | None = None) -> str:
    """Render the draft pack grid as an HTML string using the Jinja2 template.

    Queries DB for card metadata + owned quantity for all arena_ids in
    state.pack_cards, then renders partials/draft_pack.html.
    Returns a plain HTML string suitable for SSE data payload.

    active_fmt: format string override (e.g. "QuickDraft"); falls back to state.format
    active_ratings: ratings dict override; falls back to state.ratings
    active_set: set code override (e.g. "TMT"); falls back to state.set_code
    """
    fmt = active_fmt or state.format or "PremierDraft"
    display_set = active_set or state.set_code
    ratings = active_ratings if active_ratings is not None else state.ratings

    db = app.state.db
    templates = app.state.templates
    arena_ids = state.pack_cards
    ratings_available = bool(ratings) and any(
        v is not None
        for entry in ratings.values()
        for v in (entry.values() if isinstance(entry, dict) else [entry])
    )
    if not arena_ids:
        template = templates.env.get_template("partials/draft_pack.html")
        return template.render(
            phase=state.phase.name,
            set_code=display_set,
            format=fmt,
            pack_num=state.pack_num,
            pick_num=state.pick_num,
            cards=[],
            taken_set=set(),
            top_picks=[],
            metric_values={},
            metric_pct=True,
            metric_label='GIHWR',
            ratings_available=ratings_available,
        )
    placeholders = ",".join("?" * len(arena_ids))
    rows = db.execute(
        f"""
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
        SELECT c.arena_id, c.name, c.rarity, c.mana_cost, c.type_line,
               c.image_uri_back, c.is_rebalanced, c.colors,
               COALESCE(col.quantity, 0) AS owned_quantity,
               COALESCE(dd.deck_count, 0) AS deck_demand
        FROM cards c
        LEFT JOIN collection col ON col.arena_id = c.arena_id
        LEFT JOIN deck_demand dd ON dd.name_lower = LOWER(c.name)
        WHERE c.arena_id IN ({placeholders})
        """,
        arena_ids,
    ).fetchall()
    # Build dict keyed by arena_id, then sort by rarity: mythic → rare → uncommon → common
    card_map = {r["arena_id"]: dict(r) for r in rows}
    _RARITY_ORDER = {"mythic": 0, "rare": 1, "uncommon": 2, "common": 3}
    cards = sorted(
        (card_map[aid] for aid in arena_ids if aid in card_map),
        key=lambda c: _RARITY_ORDER.get((c.get("rarity") or "").lower(), 99),
    )

    metric_key = getattr(app.state, 'draft_metric', 'gihwr')
    metric_cfg = METRICS.get(metric_key, METRICS['gihwr'])
    field_name = metric_cfg['field']
    higher_better = metric_cfg['higher_better']
    color_filter = getattr(app.state, 'draft_color_filter', set())

    def _get_val(aid):
        return ratings.get(aid, {}).get(field_name)

    def _on_color(aid):
        if not color_filter:
            return True
        colors = json.loads(card_map.get(aid, {}).get('colors') or '[]')
        return not colors or bool(set(colors) & color_filter)

    rated = [(aid, _get_val(aid)) for aid in arena_ids if _get_val(aid) is not None]
    if color_filter:
        on_color  = sorted([(a, r) for a, r in rated if     _on_color(a)], key=lambda x: x[1], reverse=higher_better)
        off_color = sorted([(a, r) for a, r in rated if not _on_color(a)], key=lambda x: x[1], reverse=higher_better)
        ranked = on_color + off_color
    else:
        ranked = sorted(rated, key=lambda x: x[1], reverse=higher_better)
    top_picks = [aid for aid, _ in ranked[:3]]

    # Build display values for template overlay
    metric_values = {str(aid): _get_val(aid) for aid in arena_ids}

    template = templates.env.get_template("partials/draft_pack.html")
    return template.render(
        phase=state.phase.name,
        set_code=display_set,
        format=fmt,
        pack_num=state.pack_num,
        pick_num=state.pick_num,
        cards=cards,
        taken_set=set(state.taken_cards),
        top_picks=top_picks,
        metric_values=metric_values,
        metric_pct=metric_cfg['pct'],
        metric_label=metric_cfg['label'],
        ratings_available=ratings_available,
    )


# ---------------------------------------------------------------------------
# Watchdog bridge — mirrors CollectionFileHandler in src/watcher.py
# ---------------------------------------------------------------------------

class LogScanner(FileSystemEventHandler):
    """Bridges watchdog OS-thread log file events to the asyncio event loop queue."""

    def __init__(self, loop: asyncio.AbstractEventLoop, queue: asyncio.Queue, log_path: Path):
        self._loop = loop
        self._queue = queue
        self._target = str(log_path.resolve())

    def on_modified(self, event):
        if event.is_directory:
            return
        if str(Path(event.src_path).resolve()) != self._target:
            return
        # Only thread-safe call allowed from a non-asyncio thread:
        self._loop.call_soon_threadsafe(self._queue.put_nowait, event.src_path)


# ---------------------------------------------------------------------------
# Asyncio consumer coroutine
# ---------------------------------------------------------------------------

async def log_consumer(app, queue: asyncio.Queue, state: DraftState) -> None:
    """Dequeue log-change events, read new bytes, parse draft events, update state.

    Pushes serialized pack state to app.state.draft_event_queue when state changes.
    Must run as an asyncio task — never call from a watchdog OS thread.
    """
    while True:
        path_str = await queue.get()
        log_path = Path(path_str)
        try:
            # Log rotation guard: if stored offset > current file size, reset
            file_size = log_path.stat().st_size
            if state.file_offset > file_size:
                logger.info("log_scanner: log rotation detected — resetting offset to 0")
                state.file_offset = 0
            # Read new bytes only
            with open(log_path, encoding="utf-8", errors="replace") as f:
                f.seek(state.file_offset)
                new_lines = f.readlines()
                state.file_offset = f.tell()
            prev_phase = state.phase
            changed = False
            for line in new_lines:
                changed |= _process_line(line, state)
            # Fetch 17lands ratings whenever we're in a draft and have no ratings yet.
            # state.ratings is cleared on each new draft start, so this correctly
            # re-fetches when switching formats without restarting the scanner.
            set_override = getattr(app.state, 'draft_set_override', None)
            active_set = set_override or state.set_code
            if (state.phase in (DraftPhase.DRAFT_STARTED, DraftPhase.PACK_OFFERED)
                    and not state.ratings
                    and active_set):
                try:
                    state.ratings = await fetch_ratings(active_set, state.format)
                    logger.info(
                        "log_scanner: fetched %d ratings for %s %s",
                        sum(1 for v in state.ratings.values() if v is not None),
                        active_set,
                        state.format,
                    )
                except Exception:
                    logger.exception("log_scanner: failed to fetch 17lands ratings")
            if changed:
                html_string = render_pack_html(app, state, active_set=set_override)
                if hasattr(app.state, "draft_event_queue"):
                    app.state.draft_event_queue.put_nowait(html_string)
        except Exception:
            logger.exception("log_scanner: error processing log event")


# ---------------------------------------------------------------------------
# Startup helper — parallel to start_watcher() in src/watcher.py
# ---------------------------------------------------------------------------

async def start_draft_scanner(app, log_path: Path, state: DraftState) -> None:
    """Start the watchdog Observer and the asyncio log consumer coroutine.

    Stores observer and queue on app.state so shutdown() can stop the observer.
    Triggers an immediate read so any events already in the file are processed.
    """
    loop = asyncio.get_running_loop()
    queue: asyncio.Queue = asyncio.Queue()

    handler = LogScanner(loop, queue, log_path)
    observer = Observer()
    observer.schedule(handler, path=str(log_path.parent.resolve()), recursive=False)
    observer.start()

    app.state.draft_observer = observer
    app.state.draft_log_queue = queue

    # Trigger an initial read so events already written to the file are processed
    queue.put_nowait(str(log_path.resolve()))

    asyncio.create_task(log_consumer(app, queue, state))
    logger.info("log_scanner: started watching %s", log_path)
