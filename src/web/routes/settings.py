"""Settings routes: data refresh actions, path management, and SSE reload progress."""

import asyncio
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter, Form, Request
from fastapi.responses import RedirectResponse, StreamingResponse

from src.collection import find_collection_file, upsert_collection
from src.db.errata import apply_errata
from src.db.ingest import get_bulk_download_uri, download_bulk, ingest_scryfall, backfill_rebalanced_images, enrich_missing_from_api
from src.db.mtga_card_db import find_card_db, ingest_mtga_card_db
from src.db.log_parser import find_player_log, parse_log_decks
from src.db.decks import _build_arena_text_from_log_deck, import_deck
from src.config import DATA_DIR
from src.draft.log_scanner import METRICS, DEFAULT_METRIC_ORDER
from src.web.routes._set_info import invalidate_set_info_cache
from src.web.routes.cards import invalidate_filter_options_cache

router = APIRouter()

CACHE_PATH = DATA_DIR / "scryfall_all_cards.json"


def _fmt_ts(iso: str | None) -> str | None:
    """Format an ISO timestamp to a readable string, or return None."""
    if not iso:
        return None
    try:
        return datetime.fromisoformat(iso).strftime("%Y-%m-%d %H:%M UTC")
    except (ValueError, TypeError):
        return iso


def _ensure_reload_queues(app_state) -> dict:
    """Return app_state.reload_queues, creating it if absent."""
    if not hasattr(app_state, "reload_queues"):
        app_state.reload_queues = {}
    return app_state.reload_queues


@router.get("/settings")
async def settings_view(request: Request, status: str = ""):
    db = request.app.state.db
    templates = request.app.state.templates

    mtga_card_db_row = db.execute(
        "SELECT value FROM meta WHERE key = 'mtga_card_db_last_updated'"
    ).fetchone()
    scryfall_row = db.execute(
        "SELECT value FROM meta WHERE key = 'scryfall_last_updated'"
    ).fetchone()
    collection_row = db.execute(
        "SELECT value FROM meta WHERE key = 'collection_last_updated'"
    ).fetchone()
    log_row = db.execute(
        "SELECT value FROM meta WHERE key = 'log_last_updated'"
    ).fetchone()

    # Resolve current paths (meta-saved default checked first via db=db)
    card_db_path = find_card_db(db=db)
    collection_path = find_collection_file(db=db)
    log_path = find_player_log(db=db)

    def _meta_val(key):
        row = db.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
        return row["value"] if row else ""

    _metric_order_row = db.execute(
        "SELECT value FROM meta WHERE key='draft_metric_order'"
    ).fetchone()
    metric_order = (
        json.loads(_metric_order_row["value"]) if _metric_order_row
        else list(DEFAULT_METRIC_ORDER)
    )

    context = {
        "mode": "settings",
        "status": status,
        "mtga_card_db_last_updated": _fmt_ts(mtga_card_db_row["value"] if mtga_card_db_row else None),
        "scryfall_last_updated": _fmt_ts(scryfall_row["value"] if scryfall_row else None),
        "collection_last_updated": _fmt_ts(collection_row["value"] if collection_row else None),
        "log_last_updated": _fmt_ts(log_row["value"] if log_row else None),
        "card_db_resolved": str(card_db_path) if card_db_path else "Not found",
        "collection_resolved": str(collection_path) if collection_path else "Not found",
        "log_resolved": str(log_path) if log_path else "Not found",
        "scryfall_cache": str(CACHE_PATH),
        "default_card_db_path": _meta_val("default_card_db_path"),
        "default_collection_path": _meta_val("default_collection_path"),
        "default_log_path": _meta_val("default_log_path"),
        "metric_order": metric_order,
        "METRICS": METRICS,
        "collection_default_sort": _meta_val("collection_default_sort") or "alpha",
        "collection_default_per_page": _meta_val("collection_default_per_page") or "20",
    }
    return templates.TemplateResponse(request, "settings.html", context)


@router.post("/settings/set-default-path")
async def set_default_path(request: Request, source: str = Form(...), path: str = Form(...)):
    """Save a persistent default file path for a given data source to meta."""
    db = request.app.state.db
    key_map = {
        "card_db": "default_card_db_path",
        "collection": "default_collection_path",
        "log": "default_log_path",
    }
    key = key_map.get(source)
    if not key:
        return RedirectResponse(url="/settings?status=err_bad_source", status_code=303)
    db.execute("INSERT OR REPLACE INTO meta VALUES (?, ?)", (key, path.strip()))
    db.commit()
    return RedirectResponse(url=f"/settings?status=default_saved_{source}", status_code=303)


@router.post("/settings/collection-defaults")
async def save_collection_defaults(
    request: Request,
    default_sort: str = Form(...),
    default_per_page: str = Form(...),
):
    """Save default sort and per-page for the collection view to meta."""
    from src.web.routes.cards import VALID_SORT, VALID_PER_PAGE
    db = request.app.state.db
    sort = default_sort if default_sort in VALID_SORT else "alpha"
    per_page = default_per_page if default_per_page in {str(v) for v in VALID_PER_PAGE} else "20"
    db.execute("INSERT OR REPLACE INTO meta VALUES ('collection_default_sort', ?)", (sort,))
    db.execute("INSERT OR REPLACE INTO meta VALUES ('collection_default_per_page', ?)", (per_page,))
    db.commit()
    return RedirectResponse(url="/settings?status=collection_defaults_saved", status_code=303)


@router.get("/settings/reload-stream/{source}")
async def reload_stream(request: Request, source: str, token: str = ""):
    """SSE endpoint: streams reload progress events for the given source+token."""
    queue_key = f"{source}_{token}"

    # Wait up to 30s for the queue to appear (client may connect before POST creates it)
    queue = None
    for _ in range(60):
        queue = _ensure_reload_queues(request.app.state).get(queue_key)
        if queue is not None:
            break
        await asyncio.sleep(0.5)

    async def sse_generator():
        if queue is None:
            payload = json.dumps({"stage": "error", "message": "No reload in progress"})
            yield f"event: progress\ndata: {payload}\n\n"
            return
        while True:
            if await request.is_disconnected():
                break
            try:
                event_data = await asyncio.wait_for(queue.get(), timeout=15.0)
                yield f"event: progress\ndata: {event_data}\n\n"
                parsed = json.loads(event_data)
                if parsed.get("stage") == "done":
                    _ensure_reload_queues(request.app.state).pop(queue_key, None)
                    break
            except asyncio.TimeoutError:
                # Send a keep-alive comment to prevent client timeout
                yield ": keepalive\n\n"

    return StreamingResponse(sse_generator(), media_type="text/event-stream")


@router.post("/settings/refresh-mtga-card-db")
async def refresh_mtga_card_db(request: Request):
    db = request.app.state.db
    token = str(uuid.uuid4())[:8]
    queue: asyncio.Queue = asyncio.Queue()
    reload_queues = _ensure_reload_queues(request.app.state)
    # Clear any stale queue for this source
    for k in [k for k in reload_queues if k.startswith("mtga-card-db_")]:
        del reload_queues[k]
    reload_queues[f"mtga-card-db_{token}"] = queue

    loop = asyncio.get_event_loop()

    def _progress(stage, rows, total, message):
        pct = int(rows / total * 100) if total > 0 else 50
        payload = json.dumps({"stage": stage, "rows": rows, "pct": pct, "message": message})
        asyncio.run_coroutine_threadsafe(queue.put(payload), loop)

    async def _run():
        try:
            card_db_path = find_card_db(db=db)
            if card_db_path is None:
                await queue.put(json.dumps({"stage": "done", "pct": 0, "status": "mtga_err_no_file", "message": "MTGA Card Database not found"}))
                return
            await asyncio.get_event_loop().run_in_executor(
                None, lambda: ingest_mtga_card_db(db, card_db_path, progress_callback=_progress)
            )
            await asyncio.get_event_loop().run_in_executor(None, lambda: backfill_rebalanced_images(db))
            now = datetime.now(timezone.utc).isoformat()
            db.execute("INSERT OR REPLACE INTO meta VALUES ('mtga_card_db_last_updated', ?)", (now,))
            db.commit()
            apply_errata(db)
            invalidate_filter_options_cache()
            await queue.put(json.dumps({"stage": "done", "pct": 100, "status": "mtga_ok", "message": "MTGA Card Database updated successfully."}))
        except Exception as e:
            await queue.put(json.dumps({"stage": "done", "pct": 0, "status": "mtga_err", "message": str(e)}))

    asyncio.create_task(_run())
    return {"token": token}


@router.post("/settings/refresh-collection")
async def refresh_collection(request: Request):
    db = request.app.state.db
    token = str(uuid.uuid4())[:8]
    queue: asyncio.Queue = asyncio.Queue()
    reload_queues = _ensure_reload_queues(request.app.state)
    for k in [k for k in reload_queues if k.startswith("collection_")]:
        del reload_queues[k]
    reload_queues[f"collection_{token}"] = queue

    loop = asyncio.get_event_loop()

    def _progress(stage, rows, total, message):
        pct = int(rows / total * 100) if total > 0 else 50
        payload = json.dumps({"stage": stage, "rows": rows, "pct": pct, "message": message})
        asyncio.run_coroutine_threadsafe(queue.put(payload), loop)

    async def _run():
        try:
            collection_path = find_collection_file(db=db)
            if collection_path is None:
                await queue.put(json.dumps({"stage": "done", "pct": 0, "status": "collection_err_no_file", "message": "Collection file not found. Is Untapped running?"}))
                return
            await asyncio.get_event_loop().run_in_executor(
                None, lambda: upsert_collection(db, collection_path, progress_callback=_progress)
            )
            apply_errata(db)
            await queue.put(json.dumps({"stage": "done", "pct": 100, "status": "collection_ok", "message": "Collection updated successfully."}))
        except Exception as e:
            await queue.put(json.dumps({"stage": "done", "pct": 0, "status": "collection_err", "message": str(e)}))

    asyncio.create_task(_run())
    return {"token": token}


@router.post("/settings/refresh-scryfall")
async def refresh_scryfall(request: Request):
    db = request.app.state.db
    token = str(uuid.uuid4())[:8]
    queue: asyncio.Queue = asyncio.Queue()
    reload_queues = _ensure_reload_queues(request.app.state)
    for k in [k for k in reload_queues if k.startswith("scryfall_")]:
        del reload_queues[k]
    reload_queues[f"scryfall_{token}"] = queue

    loop = asyncio.get_event_loop()

    def _progress(stage, rows, total, message):
        pct = int(rows / total * 100) if total > 0 else (20 if stage == "download" else 50)
        payload = json.dumps({"stage": stage, "rows": rows, "pct": pct, "message": message})
        asyncio.run_coroutine_threadsafe(queue.put(payload), loop)

    async def _run():
        try:
            CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
            uri = await asyncio.get_event_loop().run_in_executor(None, get_bulk_download_uri)
            await asyncio.get_event_loop().run_in_executor(
                None, lambda: download_bulk(uri, CACHE_PATH, progress_callback=_progress)
            )

            # Build fallback maps from cards table
            rows = db.execute("SELECT arena_id, name, set_code, collector_number FROM cards").fetchall()
            arena_id_map: dict = {}
            name_set_map: dict = {}
            name_counts: dict = {}
            src_name_by_id: dict = {}
            for arena_id, name, set_code, cn in rows:
                src_name_by_id[arena_id] = name
                if set_code and cn:
                    arena_id_map[(set_code.lower(), str(cn))] = arena_id
                if name and set_code:
                    name_set_map[(name, set_code.lower())] = arena_id
                if name:
                    name_counts[name] = name_counts.get(name, 0) + 1
            name_map = {}
            for (name, set_code), arena_id in name_set_map.items():
                if name_counts.get(name, 0) == 1:
                    name_map[name] = arena_id

            rebalanced_id_map: dict = {}
            for r in db.execute("SELECT arena_id, set_code, collector_number FROM cards WHERE is_rebalanced=1").fetchall():
                if r[1] and r[2]:
                    rebalanced_id_map[(r[1].lower(), str(r[2]))] = r[0]

            await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: ingest_scryfall(
                    CACHE_PATH, db,
                    arena_id_map=arena_id_map,
                    name_set_map=name_set_map,
                    name_map=name_map,
                    src_name_by_id=src_name_by_id,
                    rebalanced_id_map=rebalanced_id_map,
                    progress_callback=_progress,
                )
            )
            await asyncio.get_event_loop().run_in_executor(None, lambda: backfill_rebalanced_images(db))
            await asyncio.get_event_loop().run_in_executor(None, lambda: enrich_missing_from_api(db, _progress))

            now = datetime.now(timezone.utc).isoformat()
            db.execute("INSERT OR REPLACE INTO meta VALUES ('scryfall_last_updated', ?)", (now,))
            db.commit()
            apply_errata(db)
            invalidate_set_info_cache()
            invalidate_filter_options_cache()
            await queue.put(json.dumps({"stage": "done", "pct": 100, "status": "scryfall_ok", "message": "Scryfall data updated successfully."}))
        except Exception as e:
            await queue.put(json.dumps({"stage": "done", "pct": 0, "status": "scryfall_err", "message": str(e)}))

    asyncio.create_task(_run())
    return {"token": token}


@router.post("/settings/load-collection-file")
async def load_collection_file(request: Request, file_path: str = Form(...)):
    path = Path(file_path.strip())
    if not path.exists():
        return RedirectResponse(
            url=f"/settings?status=custom_collection_err_no_file&file={path.name}",
            status_code=303,
        )
    try:
        upsert_collection(request.app.state.db, path)
        return RedirectResponse(
            url=f"/settings?status=custom_collection_ok&file={path.name}",
            status_code=303,
        )
    except Exception:
        return RedirectResponse(url="/settings?status=custom_collection_err", status_code=303)


@router.post("/settings/load-log-file")
async def load_log_file(request: Request, file_path: str = Form(...)):
    db = request.app.state.db
    path = Path(file_path.strip())
    if not path.exists():
        return RedirectResponse(
            url=f"/settings?status=custom_log_err_no_file&file={path.name}",
            status_code=303,
        )
    try:
        parsed_decks = parse_log_decks(path)
        if not parsed_decks:
            return RedirectResponse(url="/settings?status=custom_log_err_no_decks", status_code=303)

        imported = 0
        for deck in parsed_decks:
            lines = []
            for entry in deck["mainboard"]:
                row = db.execute(
                    "SELECT name FROM cards WHERE arena_id = ?", (entry["arena_id"],)
                ).fetchone()
                if row:
                    lines.append(f"{entry['quantity']} {row['name']}")
            if not lines:
                continue
            existing = db.execute(
                "SELECT id FROM decks WHERE name = ? AND source = 'log'",
                (deck["name"],),
            ).fetchone()
            if existing:
                db.execute("DELETE FROM deck_lines WHERE deck_id = ?", (existing["id"],))
                db.execute("DELETE FROM decks WHERE id = ?", (existing["id"],))
            arena_text = f"About\nName {deck['name']}\n\nDeck\n" + "\n".join(lines)
            deck_id = import_deck(db, text=arena_text, is_potential=False, is_saved=False)
            db.execute(
                "UPDATE decks SET source='log', format=? WHERE id=?",
                (deck.get("format") or None, deck_id),
            )
            imported += 1

        now = datetime.now(timezone.utc).isoformat()
        db.execute("INSERT OR REPLACE INTO meta VALUES ('log_last_updated', ?)", (now,))
        db.commit()
        return RedirectResponse(
            url=f"/settings?status=custom_log_ok&log_count={imported}&file={path.name}",
            status_code=303,
        )
    except Exception:
        return RedirectResponse(url="/settings?status=custom_log_err", status_code=303)


@router.post("/settings/refresh-log")
async def refresh_log(request: Request):
    db = request.app.state.db
    token = str(uuid.uuid4())[:8]
    queue: asyncio.Queue = asyncio.Queue()
    reload_queues = _ensure_reload_queues(request.app.state)
    for k in [k for k in reload_queues if k.startswith("log_")]:
        del reload_queues[k]
    reload_queues[f"log_{token}"] = queue

    loop = asyncio.get_event_loop()

    def _progress(stage, rows, total, message):
        pct = int(rows / total * 100) if total > 0 else 50
        payload = json.dumps({"stage": stage, "rows": rows, "pct": pct, "message": message})
        asyncio.run_coroutine_threadsafe(queue.put(payload), loop)

    async def _run():
        try:
            log_path = find_player_log(db=db)
            if log_path is None:
                await queue.put(json.dumps({"stage": "done", "pct": 0, "status": "log_err_no_file", "message": "Game log not found. Launch MTGA first."}))
                return
            parsed_decks = await asyncio.get_event_loop().run_in_executor(None, lambda: parse_log_decks(log_path))
            if not parsed_decks:
                await queue.put(json.dumps({"stage": "done", "pct": 0, "status": "log_err_no_decks", "message": "No decks found in game log. Open your deck list in MTGA."}))
                return

            _progress("log", 0, len(parsed_decks), f"Importing {len(parsed_decks)} deck(s)...")

            imported = 0
            for deck in parsed_decks:
                arena_text = _build_arena_text_from_log_deck(db, deck)
                if arena_text is None:
                    continue
                existing = db.execute(
                    "SELECT id FROM decks WHERE name = ? AND source = 'log'",
                    (deck["name"],),
                ).fetchone()
                if existing:
                    db.execute("DELETE FROM deck_lines WHERE deck_id = ?", (existing["id"],))
                    db.execute("DELETE FROM decks WHERE id = ?", (existing["id"],))
                deck_id = import_deck(db, text=arena_text, is_potential=False, is_saved=False)
                db.execute(
                    "UPDATE decks SET source='log', format=? WHERE id=?",
                    (deck.get("format") or None, deck_id),
                )
                imported += 1

            now = datetime.now(timezone.utc).isoformat()
            db.execute("INSERT OR REPLACE INTO meta VALUES ('log_last_updated', ?)", (now,))
            db.commit()
            apply_errata(db)
            await queue.put(json.dumps({"stage": "done", "pct": 100, "status": "log_ok", "message": f"Imported {imported} deck(s) from game log."}))
        except Exception as e:
            await queue.put(json.dumps({"stage": "done", "pct": 0, "status": "log_err", "message": str(e)}))

    asyncio.create_task(_run())
    return {"token": token}


@router.post("/settings/draft-metric-order")
async def save_draft_metric_order(request: Request):
    """Save draft metric display order to meta and app state."""
    form = await request.form()
    order = form.getlist("order")
    valid = [k for k in order if k in METRICS]
    # add any missing keys at end
    for k in DEFAULT_METRIC_ORDER:
        if k not in valid:
            valid.append(k)
    db = request.app.state.db
    db.execute(
        "INSERT OR REPLACE INTO meta (key,value) VALUES ('draft_metric_order',?)",
        (json.dumps(valid),)
    )
    db.commit()
    request.app.state.draft_metric_order = valid
    return RedirectResponse("/settings", status_code=303)
