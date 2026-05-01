"""Deck management routes: list, import, detail, export, rename, delete, save actions."""

from __future__ import annotations

import asyncio
import json
import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, Form, HTTPException, Request
from fastapi.responses import JSONResponse, PlainTextResponse, Response, StreamingResponse
from starlette.responses import RedirectResponse

from src.db.deck_scan import apply_changed_deck_lines, compute_content_hash, create_version, scan_log_decks
from src.db.log_parser import find_player_log, parse_log_decks

from src.db.decks import (
    _build_arena_text_from_log_deck,
    add_deck_line,
    bulk_delete,
    bulk_move_to_potential,
    bulk_move_to_saved,
    copy_deck_to_saved,
    delete_deck,
    export_deck_to_arena,
    get_deck,
    get_deck_lines,
    get_deck_version,
    get_deck_versions,
    get_version_lines,
    import_deck,
    list_decks,
    move_to_saved,
    remove_deck_line,
    rename_deck,
    replace_deck_from_text,
    resolve_card_name,
    restore_from_version,
    save_for_later,
    unsave_deck,
    update_deck_format,
    update_deck_line_qty,
)

router = APIRouter()

# Type grouping for Moxfield-style deck view
_TYPE_ORDER = ['Planeswalker', 'Creature', 'Artifact', 'Enchantment', 'Instant', 'Sorcery', 'Land']
_TYPE_LABELS = {
    'Planeswalker': 'Planeswalkers', 'Creature': 'Creatures', 'Artifact': 'Artifacts',
    'Enchantment': 'Enchantments', 'Instant': 'Instants', 'Sorcery': 'Sorceries',
    'Land': 'Lands', 'Other': 'Other',
}


def _type_group(type_line: str | None) -> str:
    if not type_line:
        return 'Other'
    for t in _TYPE_ORDER:
        if t in type_line:
            return t
    return 'Other'


@router.post("/decks/refresh-decks")
async def refresh_decks(request: Request):
    """Start a deck log scan. Returns SSE token for /decks/scan-stream."""
    db = request.app.state.db
    token = str(uuid.uuid4())[:8]
    queue: asyncio.Queue = asyncio.Queue()

    if not hasattr(request.app.state, "scan_queues"):
        request.app.state.scan_queues = {}
    request.app.state.scan_queues.clear()
    request.app.state.scan_queues[token] = queue

    loop = asyncio.get_event_loop()

    async def _run():
        try:
            log_path = find_player_log(db=db)
            if log_path is None:
                await queue.put(json.dumps({"stage": "done", "pct": 0, "status": "scan_err_no_file", "message": "Game log not found. Launch MTGA first."}))
                return

            await queue.put(json.dumps({"stage": "scan", "pct": 10, "message": "Scanning log..."}))
            result = await asyncio.get_event_loop().run_in_executor(
                None, lambda: scan_log_decks(db, log_path)
            )

            await queue.put(json.dumps({
                "stage": "scan",
                "pct": 60,
                "message": f"Found {len(result.new_decks)} new, {len(result.changed_decks)} changed, {len(result.missing_decks)} missing."
            }))

            request.app.state.pending_scan = result

            now = datetime.now(timezone.utc).isoformat()
            db.execute("INSERT OR REPLACE INTO meta VALUES ('log_last_updated', ?)", (now,))
            db.commit()

            await queue.put(json.dumps({
                "stage": "done", "pct": 100, "status": "scan_ok",
                "message": f"Scan complete: {len(result.new_decks)} new, {len(result.changed_decks)} changed, {len(result.missing_decks)} missing."
            }))
        except Exception as e:
            await queue.put(json.dumps({"stage": "done", "pct": 0, "status": "scan_err", "message": str(e)}))

    asyncio.create_task(_run())
    return {"token": token}


@router.get("/decks/scan-stream")
async def scan_stream(request: Request, token: str = ""):
    """SSE stream for deck scan progress. Pair with POST /decks/refresh-decks."""
    queue = None
    scan_queues = getattr(request.app.state, "scan_queues", {})
    for _ in range(60):
        queue = scan_queues.get(token)
        if queue is not None:
            break
        await asyncio.sleep(0.5)

    async def sse_generator():
        if queue is None:
            yield f"event: progress\ndata: {json.dumps({'stage': 'done', 'pct': 0, 'status': 'scan_err', 'message': 'Scan not found or expired.'})}\n\n"
            return
        while True:
            if await request.is_disconnected():
                break
            try:
                event_data = await asyncio.wait_for(queue.get(), timeout=15.0)
                yield f"event: progress\ndata: {event_data}\n\n"
                if json.loads(event_data).get("stage") == "done":
                    scan_queues.pop(token, None)
                    break
            except asyncio.TimeoutError:
                yield ": keepalive\n\n"

    return StreamingResponse(sse_generator(), media_type="text/event-stream")


@router.get("/decks/scan-review")
async def scan_review(request: Request):
    """Return dialog partial HTML for pending scan decisions. Returns 204 if nothing to review."""
    result = getattr(request.app.state, "pending_scan", None)
    if result is None or (not result.new_decks and not result.changed_decks and not result.missing_decks):
        return Response(status_code=204)
    return request.app.state.templates.TemplateResponse(
        request,
        "partials/scan_decision_dialog.html",
        {
            "new_decks": result.new_decks,
            "changed_decks": result.changed_decks,
            "missing_decks": result.missing_decks,
        },
    )


@router.post("/decks/scan-decisions")
async def scan_decisions(request: Request):
    """Apply all scan decisions atomically. Nothing touches the DB until this is called."""
    db = request.app.state.db
    body = await request.json()
    changed_decisions: dict[str, str] = body.get("changed", {})
    missing_decisions: dict[str, str] = body.get("missing", {})

    result = getattr(request.app.state, "pending_scan", None)
    if result is None:
        return JSONResponse({"error": "No pending scan"}, status_code=400)

    changed_by_id = {str(d.deck_id): d for d in result.changed_decks}
    missing_by_id = {str(d.deck_id): d for d in result.missing_decks}

    counts = {"imported": 0, "versioned": 0, "replaced": 0, "archived": 0, "deleted": 0}

    # Import all new decks (no user choice — all or nothing with Apply All)
    for parsed in result.new_decks:
        arena_text = _build_arena_text_from_log_deck(db, {
            "name": parsed.name,
            "mainboard": parsed.mainboard,
            "sideboard": parsed.sideboard,
            "commander": parsed.commander,
            "format": parsed.format,
        })
        if arena_text is None:
            continue
        deck_id = import_deck(db, text=arena_text, is_potential=False, is_saved=False)
        db.execute(
            "UPDATE decks SET source='log', format=?, log_deck_id=?, content_hash=? WHERE id=?",
            (parsed.format or None, parsed.log_deck_id, compute_content_hash(parsed.mainboard), deck_id),
        )
        counts["imported"] += 1

    # Handle changed decks
    for deck_id_str, action in changed_decisions.items():
        if deck_id_str not in changed_by_id:
            continue
        changed = changed_by_id[deck_id_str]
        if action == "add_version":
            create_version(db, changed.deck_id)
            apply_changed_deck_lines(db, changed.deck_id, changed.parsed)
            db.execute("UPDATE decks SET content_hash=? WHERE id=?", (changed.new_hash, changed.deck_id))
            counts["versioned"] += 1
        elif action == "replace":
            db.execute("DELETE FROM decks WHERE id=?", [changed.deck_id])
            arena_text = _build_arena_text_from_log_deck(db, {
                "name": changed.parsed.name,
                "mainboard": changed.parsed.mainboard,
                "sideboard": changed.parsed.sideboard,
                "commander": changed.parsed.commander,
                "format": changed.parsed.format,
            })
            if arena_text:
                new_id = import_deck(db, text=arena_text, is_potential=False, is_saved=False)
                db.execute(
                    "UPDATE decks SET source='log', format=?, log_deck_id=?, content_hash=? WHERE id=?",
                    (changed.parsed.format or None, changed.log_deck_id, changed.new_hash, new_id),
                )
            counts["replaced"] += 1

    # Handle missing decks
    for deck_id_str, action in missing_decisions.items():
        if deck_id_str not in missing_by_id:
            continue
        deck_id = int(deck_id_str)
        if action == "archive":
            db.execute("UPDATE decks SET is_saved=1 WHERE id=?", [deck_id])
            counts["archived"] += 1
        elif action == "delete":
            db.execute("DELETE FROM decks WHERE id=?", [deck_id])
            counts["deleted"] += 1

    db.commit()
    request.app.state.pending_scan = None
    return JSONResponse(counts)


@router.post("/decks/cancel-scan")
async def cancel_scan(request: Request):
    """Discard pending scan result without making any DB changes."""
    request.app.state.pending_scan = None
    return JSONResponse({"ok": True})


@router.get("/decks")
async def deck_list(request: Request, potential: int = 0, saved: int = 0, sort: str = "name"):
    """Deck list view. ?potential=1 for potential, ?saved=1 for saved, ?sort=name|name_desc|cards|imported."""
    db = request.app.state.db
    templates = request.app.state.templates

    is_saved = bool(saved)
    is_potential = bool(potential) and not is_saved

    decks = list_decks(db, is_potential=is_potential, is_saved=is_saved, sort=sort)

    if is_saved:
        mode = "saved"
    elif is_potential:
        mode = "potential"
    else:
        mode = "decks"

    context = {
        "decks": decks,
        "is_potential": is_potential,
        "is_saved": is_saved,
        "mode": mode,
        "sort": sort,
    }

    if request.headers.get("HX-Request"):
        return templates.TemplateResponse(request, "partials/deck_list.html", context)
    return templates.TemplateResponse(request, "decks.html", context)


@router.post("/decks/bulk")
async def bulk_action(request: Request):
    """Bulk action on selected decks. Form fields: ids (multiple), action."""
    db = request.app.state.db
    form = await request.form()
    ids = [int(i) for i in form.getlist("ids") if i.isdigit()]
    action = form.get("action", "")

    if ids:
        if action == "delete":
            bulk_delete(db, ids)
        elif action == "move-to-saved":
            bulk_move_to_saved(db, ids)
        elif action == "move-to-potential":
            bulk_move_to_potential(db, ids)

    # Redirect back to whichever tab the user was on
    referer = request.headers.get("referer", "/decks")
    return RedirectResponse(url=referer, status_code=303)


@router.post("/decks/import")
async def import_deck_route(request: Request):
    """Import an Arena text decklist. All imports are potential decks."""
    db = request.app.state.db

    try:
        form = await request.form()
        text = form.get("decklist", "")
        mode = form.get("mode", "potential")
        is_saved = mode == "saved"
        # Arena Decks tab falls back to potential — can't manually import as arena
        deck_id = import_deck(db, text=text, is_potential=not is_saved, is_saved=is_saved)
        return Response(status_code=204, headers={"HX-Redirect": f"/decks/{deck_id}"})
    except Exception as e:
        return Response(status_code=422, content=str(e))


@router.post("/decks/import-from-log")
async def import_decks_from_log(request: Request):
    """Import all Arena decks from Player.log into the database."""
    db = request.app.state.db
    log_path = find_player_log()
    if log_path is None:
        return RedirectResponse(url="/decks?log_err=no_file", status_code=303)
    try:
        parsed_decks = parse_log_decks(log_path)
        if not parsed_decks:
            return RedirectResponse(url="/decks?log_err=no_decks", status_code=303)

        imported = 0
        for deck in parsed_decks:
            arena_text = _build_arena_text_from_log_deck(db, deck)
            if arena_text is None:
                continue

            # Upsert: delete existing log deck with same name before reimporting
            existing = db.execute(
                "SELECT id FROM decks WHERE name = ? AND source = 'log'",
                (deck["name"],),
            ).fetchone()
            if existing:
                db.execute("DELETE FROM deck_lines WHERE deck_id = ?", (existing["id"],))
                db.execute("DELETE FROM decks WHERE id = ?", (existing["id"],))

            deck_id = import_deck(db, text=arena_text, is_potential=False, is_saved=False)

            # Patch source and format which import_deck() doesn't expose
            db.execute(
                "UPDATE decks SET source='log', format=? WHERE id=?",
                (deck.get("format") or None, deck_id),
            )
            imported += 1

        # Write log_last_updated meta key
        now = datetime.now(timezone.utc).isoformat()
        db.execute("INSERT OR REPLACE INTO meta VALUES ('log_last_updated', ?)", (now,))
        db.commit()

        return RedirectResponse(url=f"/decks?log_ok={imported}", status_code=303)
    except Exception:
        return RedirectResponse(url="/decks?log_err=parse_failed", status_code=303)


@router.post("/decks/{deck_id}/versions/{version_id}/restore")
async def restore_version(request: Request, deck_id: int, version_id: int):
    """Snapshot current state then restore deck_lines from a historical version."""
    db = request.app.state.db
    deck = get_deck(db, deck_id)
    if deck is None:
        raise HTTPException(status_code=404, detail=f"Deck {deck_id} not found")
    if not (deck["is_potential"] or deck["is_saved"]):
        raise HTTPException(status_code=403, detail="Deck is not editable")
    restore_from_version(db, deck_id, version_id)
    return Response(status_code=204, headers={"HX-Redirect": f"/decks/{deck_id}"})


@router.get("/decks/{deck_id}/export")
async def export_deck(request: Request, deck_id: int):
    """Export deck as Arena-format plain text file."""
    db = request.app.state.db

    deck = get_deck(db, deck_id)
    if deck is None:
        raise HTTPException(status_code=404, detail=f"Deck {deck_id} not found")

    arena_text = export_deck_to_arena(db, deck_id)
    filename = deck["name"].replace('"', "").replace("/", "-")
    return PlainTextResponse(
        content=arena_text,
        headers={"Content-Disposition": f'attachment; filename="{filename}.txt"'},
    )


@router.get("/decks/{deck_id}")
async def deck_detail(request: Request, deck_id: int, version_id: int = None):
    """Deck detail view — Moxfield-style type-grouped layout."""
    db = request.app.state.db
    templates = request.app.state.templates

    deck = get_deck(db, deck_id)
    if deck is None:
        raise HTTPException(status_code=404, detail=f"Deck {deck_id} not found")

    if version_id is not None:
        version = get_deck_version(db, version_id)
        if version is None or version["deck_id"] != deck_id:
            raise HTTPException(status_code=404, detail=f"Version {version_id} not found for deck {deck_id}")
        lines = get_version_lines(db, version_id)
        viewing_version = True
    else:
        lines = get_deck_lines(db, deck_id)
        viewing_version = False
        version = None

    versions = get_deck_versions(db, deck_id)

    # Group mainboard lines by card type; keep other sections separate
    mainboard_by_type: dict[str, list] = {}
    other_sections: dict[str, list] = {}
    first_arena_id = None

    for line in lines:
        line = dict(line)
        if not viewing_version:
            # Re-resolve unmatched cards on detail load (Scryfall data may have been updated)
            if not line.get("arena_id"):
                resolved = resolve_card_name(db, line["card_name"])
                if resolved:
                    db.execute("UPDATE deck_lines SET arena_id = ? WHERE id = ?", [resolved, line["id"]])
                    db.commit()
                    line["arena_id"] = resolved
                    line["display_arena_id"] = resolved
                    # Re-fetch type_line now that arena_id is resolved
                    card = db.execute("SELECT type_line, mana_cost, rarity FROM cards WHERE arena_id = ?", [resolved]).fetchone()
                    if card:
                        line["type_line"] = card["type_line"]
                        line["mana_cost"] = card["mana_cost"]
                        line["rarity"] = card["rarity"]
        line["is_missing"] = line["total_owned"] < line["required"]
        line["display_owned"] = min(line["total_owned"], line["required"])
        if first_arena_id is None and line.get("display_arena_id"):
            first_arena_id = line["display_arena_id"]
        if line["section"] == "mainboard":
            tg = _type_group(line.get("type_line"))
            mainboard_by_type.setdefault(tg, []).append(line)
        else:
            other_sections.setdefault(line["section"], []).append(line)

    groups = []
    for section in ["commander", "companion"]:
        if section in other_sections:
            grp_lines = other_sections[section]
            groups.append({
                "label": section.capitalize(),
                "lines": grp_lines,
                "count": sum(l["quantity"] for l in grp_lines),
            })
    for t in _TYPE_ORDER + ["Other"]:
        if t in mainboard_by_type:
            grp_lines = mainboard_by_type[t]
            groups.append({
                "label": _TYPE_LABELS[t],
                "lines": grp_lines,
                "count": sum(l["quantity"] for l in grp_lines),
            })
    for section in ["sideboard"]:
        if section in other_sections:
            grp_lines = other_sections[section]
            groups.append({
                "label": section.capitalize(),
                "lines": grp_lines,
                "count": sum(l["quantity"] for l in grp_lines),
            })

    # Missing card summary by section and rarity
    RARITIES = ["common", "uncommon", "rare", "mythic", "unknown"]
    missing_summary = {
        "mainboard": {r: 0 for r in RARITIES},
        "sideboard": {r: 0 for r in RARITIES},
    }
    all_lines = [l for grp in groups for l in grp["lines"]]
    for line in all_lines:
        if line["is_missing"]:
            missing = line["required"] - line["total_owned"]
            rarity = (line.get("rarity") or "").lower()
            if rarity not in ("common", "uncommon", "rare", "mythic"):
                rarity = "unknown"
            bucket = "sideboard" if line["section"] == "sideboard" else "mainboard"
            missing_summary[bucket][rarity] += missing

    if deck["is_saved"]:
        mode = "saved"
    elif deck["is_potential"]:
        mode = "potential"
    else:
        mode = "decks"

    context = {
        "deck": deck,
        "groups": groups,
        "first_arena_id": first_arena_id,
        "mode": mode,
        "show_owned": True,
        "missing_summary": missing_summary,
        "arena_text": export_deck_to_arena(db, deck_id),
        "viewing_version": viewing_version,
        "current_version": version,
        "versions": versions,
    }

    return templates.TemplateResponse(request, "deck_detail.html", context)


@router.patch("/decks/{deck_id}/rename")
async def rename_deck_route(request: Request, deck_id: int):
    """Rename a deck. Expects form field 'name'."""
    db = request.app.state.db

    deck = get_deck(db, deck_id)
    if deck is None:
        raise HTTPException(status_code=404, detail=f"Deck {deck_id} not found")

    form = await request.form()
    name = (form.get("name") or "").strip()
    if not name:
        raise HTTPException(status_code=422, detail="Name cannot be empty")

    rename_deck(db, deck_id, name)
    return Response(status_code=204)


@router.patch("/decks/{deck_id}/update-format")
async def update_format_route(request: Request, deck_id: int):
    """Update the format/deck-type field. Deck must be editable."""
    db = request.app.state.db
    deck = get_deck(db, deck_id)
    if deck is None:
        raise HTTPException(status_code=404, detail=f"Deck {deck_id} not found")
    if not (deck["is_potential"] or deck["is_saved"]):
        raise HTTPException(status_code=403, detail="Deck is not editable")
    form = await request.form()
    fmt = (form.get("format") or "").strip() or None
    update_deck_format(db, deck_id, fmt)
    return Response(status_code=204)


@router.post("/decks/{deck_id}/save")
async def save_for_later_route(request: Request, deck_id: int):
    """Flag an arena deck as saved-for-later."""
    db = request.app.state.db
    if get_deck(db, deck_id) is None:
        raise HTTPException(status_code=404, detail=f"Deck {deck_id} not found")
    save_for_later(db, deck_id)
    return Response(status_code=204, headers={"HX-Redirect": f"/decks/{deck_id}"})


@router.post("/decks/{deck_id}/move-to-saved")
async def move_to_saved_route(request: Request, deck_id: int):
    """Move a potential deck to saved."""
    db = request.app.state.db
    if get_deck(db, deck_id) is None:
        raise HTTPException(status_code=404, detail=f"Deck {deck_id} not found")
    move_to_saved(db, deck_id)
    return Response(status_code=204, headers={"HX-Redirect": f"/decks/{deck_id}"})


@router.post("/decks/{deck_id}/unsave")
async def unsave_route(request: Request, deck_id: int):
    """Remove saved flag from a deck."""
    db = request.app.state.db
    if get_deck(db, deck_id) is None:
        raise HTTPException(status_code=404, detail=f"Deck {deck_id} not found")
    unsave_deck(db, deck_id)
    return Response(status_code=204, headers={"HX-Redirect": f"/decks/{deck_id}"})


@router.patch("/decks/{deck_id}/lines/{line_id}")
async def update_line_qty_route(request: Request, deck_id: int, line_id: int, quantity: int = Form(...)):
    """Update the quantity of a card line. Setting quantity to 0 removes the line."""
    db = request.app.state.db

    deck = get_deck(db, deck_id)
    if deck is None:
        raise HTTPException(status_code=404, detail=f"Deck {deck_id} not found")
    if not (deck["is_potential"] or deck["is_saved"]):
        raise HTTPException(status_code=403, detail="Deck is not editable")

    update_deck_line_qty(db, line_id, quantity)
    return Response(status_code=204, headers={"HX-Redirect": f"/decks/{deck_id}"})


@router.delete("/decks/{deck_id}/lines/{line_id}")
async def delete_line_route(request: Request, deck_id: int, line_id: int):
    """Remove a card line from a deck."""
    db = request.app.state.db

    deck = get_deck(db, deck_id)
    if deck is None:
        raise HTTPException(status_code=404, detail=f"Deck {deck_id} not found")
    if not (deck["is_potential"] or deck["is_saved"]):
        raise HTTPException(status_code=403, detail="Deck is not editable")

    remove_deck_line(db, line_id)
    return Response(status_code=204, headers={"HX-Redirect": f"/decks/{deck_id}"})


@router.post("/decks/{deck_id}/lines")
async def add_line_route(
    request: Request,
    deck_id: int,
    card_name: str = Form(...),
    quantity: int = Form(1),
    section: str = Form("mainboard"),
):
    """Add a new card line to a deck by card name."""
    db = request.app.state.db

    deck = get_deck(db, deck_id)
    if deck is None:
        raise HTTPException(status_code=404, detail=f"Deck {deck_id} not found")
    if not (deck["is_potential"] or deck["is_saved"]):
        raise HTTPException(status_code=403, detail="Deck is not editable")

    result = add_deck_line(db, deck_id, card_name, quantity, section)
    if result is None:
        return JSONResponse(status_code=422, content={"detail": f"Card not found: {card_name}"})
    return Response(status_code=204, headers={"HX-Redirect": f"/decks/{deck_id}"})


@router.delete("/decks/{deck_id}")
async def delete_deck_route(request: Request, deck_id: int):
    """Delete a deck. Returns 204 with HX-Redirect to /decks."""
    db = request.app.state.db

    deck = get_deck(db, deck_id)
    if deck is None:
        raise HTTPException(status_code=404, detail=f"Deck {deck_id} not found")

    delete_deck(db, deck_id)
    return Response(status_code=204, headers={"HX-Redirect": "/decks"})


@router.post("/decks/{deck_id}/replace")
async def replace_deck_route(request: Request, deck_id: int):
    """Replace all deck lines from Arena text. Deck must be editable (potential or saved).

    Optional form fields:
      create_version=1  — snapshot current lines as a version before replacing
      format            — update the deck's format/type field after replacing
    """
    db = request.app.state.db

    deck = get_deck(db, deck_id)
    if deck is None:
        raise HTTPException(status_code=404, detail=f"Deck {deck_id} not found")
    if not (deck["is_potential"] or deck["is_saved"]):
        raise HTTPException(status_code=403, detail="Deck is not editable")

    form = await request.form()
    text = form.get("decklist", "")
    if not text.strip():
        raise HTTPException(status_code=422, detail="Decklist cannot be empty")

    should_version = form.get("create_version", "") == "1"
    fmt = (form.get("format") or "").strip() or None

    if should_version:
        # create_version does NOT commit — replace_deck_from_text commits
        create_version(db, deck_id)

    # replace_deck_from_text calls db.commit() internally
    replace_deck_from_text(db, deck_id, text)

    if fmt is not None:
        update_deck_format(db, deck_id, fmt)

    return Response(status_code=204, headers={"HX-Redirect": f"/decks/{deck_id}"})


@router.post("/decks/{deck_id}/copy-to-saved")
async def copy_to_saved_route(request: Request, deck_id: int):
    """Create an editable saved-deck copy of any deck (e.g. an arena log deck)."""
    db = request.app.state.db

    if get_deck(db, deck_id) is None:
        raise HTTPException(status_code=404, detail=f"Deck {deck_id} not found")

    new_id = copy_deck_to_saved(db, deck_id)
    return Response(status_code=204, headers={"HX-Redirect": f"/decks/{new_id}"})
