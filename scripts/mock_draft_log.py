"""Write fake MTGA draft log events to data/Player.log for end-to-end testing.

Emits realistic draft log lines covering all supported MTGA draft formats.

Usage:
    .venv/bin/python scripts/mock_draft_log.py [set_code] [--format quick|premier|traditional|autopick]

    e.g.  .venv/bin/python scripts/mock_draft_log.py fin
          .venv/bin/python scripts/mock_draft_log.py mkm --format premier
          .venv/bin/python scripts/mock_draft_log.py fin --format autopick

Formats:
  quick        — Quick Draft / BotDraft (default): Shape C packs + BotDraft_DraftPick lines
  premier      — Premier Draft: Event_Join start + Shape A P1P1 + Shape B subsequent + V2 pick
  traditional  — Traditional Draft: Event_Join start + Shape A P1P1 + Shape B subsequent + V1 pick
  autopick     — Auto-pick simulation: Event_Join start + Shape D (LogBusinessEvents) combined lines

This exercises the full pipeline:
  log file → watchdog → log_consumer → DraftState → render_pack_html → SSE → browser
"""

import argparse
import json
import random
import sqlite3
import sys
from datetime import date
from pathlib import Path

LOG_PATH = Path("data/Player.log")
DB_PATH = Path(__file__).parent.parent / "data" / "mtga_collection.db"

parser = argparse.ArgumentParser(description="Emit fake MTGA draft log events")
parser.add_argument("set_code", nargs="?", default="fin", help="Set code (e.g. fin, mkm)")
parser.add_argument(
    "--format",
    dest="fmt",
    choices=["quick", "premier", "traditional", "autopick"],
    default="quick",
    help="Draft format to simulate (default: quick)",
)
args = parser.parse_args()
SET_CODE = args.set_code.lower()


def get_cards(set_code: str) -> list[int]:
    db = sqlite3.connect(str(DB_PATH))
    rows = db.execute(
        "SELECT arena_id FROM cards WHERE set_code = ? ORDER BY arena_id",
        (set_code,),
    ).fetchall()
    db.close()
    ids = [r[0] for r in rows]
    if not ids:
        print(f"Error: no cards found for set '{set_code}' in {DB_PATH}", file=sys.stderr)
        sys.exit(1)
    return ids


def build_packs(arena_ids: list[int]) -> list[list[int]]:
    if len(arena_ids) >= 42:
        sampled = random.sample(arena_ids, 42)
    else:
        sampled = random.choices(arena_ids, k=42)
    return [sampled[0:14], sampled[14:28], sampled[28:42]]


def write(line: str):
    with open(LOG_PATH, "a", encoding="utf-8") as f:
        f.write(line + "\n")


# ---------------------------------------------------------------------------
# Existing Quick Draft emit functions (Shape C + BotDraft pick)
# ---------------------------------------------------------------------------

def emit_pack(pack_cards: list[int], pack_num: int, pick_num: int, event_name: str):
    """Emit a BotDraft pack line matching real MTGA Quick Draft format (Shape C).

    PackNumber and PickNumber are 0-indexed, matching real logs.
    """
    payload = json.dumps({
        "Result": "Success",
        "EventName": event_name,
        "DraftStatus": "PickNext",
        "PackNumber": pack_num,
        "PickNumber": pick_num,
        "NumCardsToPick": 1,
        "DraftPack": [str(c) for c in pack_cards],
        "PackStyles": [],
        "PickedCards": [],
        "PickedStyles": [],
    })
    outer = json.dumps({"CurrentModule": "BotDraft", "Payload": payload})
    write(outer)
    print(f"  pack {pack_num + 1} pick {pick_num + 1} → {len(pack_cards)} cards offered")


def emit_pick(arena_id: int):
    """Emit a BotDraft pick line matching real MTGA Quick Draft format."""
    payload = json.dumps({"PickInfo": {"CardId": arena_id}})
    outer = json.dumps({"Payload": payload})
    write(f"[UnityCrossThreadLogger]==> BotDraft_DraftPick {outer}")


# ---------------------------------------------------------------------------
# Premier / Traditional / Autopick emit functions
# ---------------------------------------------------------------------------

def emit_premier_start(event_name: str):
    """Emit Event_Join start line for Premier/Traditional Draft."""
    payload_inner = json.dumps({"EventName": event_name})
    payload_outer = json.dumps({"Payload": payload_inner})
    outer = json.dumps({"request": payload_outer})
    write(f"[UnityCrossThreadLogger]==> Event_Join {outer}")


def emit_shape_a(pack_cards: list[int], pack_num: int, pick_num: int):
    """Emit a Shape A P1P1 CardsInPack line for Premier/Traditional Draft."""
    data = json.dumps({
        "CardsInPack": pack_cards,
        "PackNumber": pack_num,
        "PickNumber": pick_num,
    })
    write(data)
    print(f"  pack {pack_num + 1} pick {pick_num + 1} → {len(pack_cards)} cards offered (Shape A)")


def emit_shape_b(pack_cards: list[int], pack_num: int, pick_num: int):
    """Emit a Shape B Draft.Notify line for Premier/Traditional Draft (post-P1P1)."""
    data = json.dumps({
        "PackCards": ",".join(str(c) for c in pack_cards),
        "SelfPack": pack_num,
        "SelfPick": pick_num,
    })
    write(f"[UnityCrossThreadLogger]Draft.Notify {data}")
    print(f"  pack {pack_num + 1} pick {pick_num + 1} → {len(pack_cards)} cards offered (Shape B)")


def emit_pick_v2(arena_id: int):
    """Emit a Draft.MakeHumanDraftPick line for Premier Draft (V2)."""
    data = json.dumps({"cardId": arena_id})
    write(f"[UnityCrossThreadLogger]==> Draft.MakeHumanDraftPick {data}")


def emit_pick_v1(arena_id: int):
    """Emit an Event_PlayerDraftMakePick line for Traditional Draft (V1)."""
    payload = json.dumps({"GrpId": arena_id})
    request = json.dumps({"Payload": payload})
    outer = json.dumps({"request": request})
    write(f"[UnityCrossThreadLogger]==> Event_PlayerDraftMakePick {outer}")


def emit_shape_d(pack_cards: list[int], picked_id: int, event_name: str, pack_num: int, pick_num: int):
    """Emit a Shape D LogBusinessEvents combined pack+pick line (auto-pick)."""
    data = json.dumps({
        "EventId": event_name,
        "PackNumber": pack_num,
        "PickNumber": pick_num,
        "CardsInPack": pack_cards,
        "PickGrpId": picked_id,
        "AutoPick": True,
        "TimeRemainingOnPick": 0,
    })
    write(f"[UnityCrossThreadLogger]<==LogBusinessEvents {data}")
    print(f"  auto-pick P{pack_num + 1}P{pick_num + 1} — picked {picked_id} (Shape D)")


# ---------------------------------------------------------------------------
# Main dispatcher
# ---------------------------------------------------------------------------

def main():
    arena_ids = get_cards(SET_CODE)
    print(f"Loaded {len(arena_ids)} {SET_CODE.upper()} cards from DB")

    packs = build_packs(arena_ids)
    pack_states = [list(p) for p in packs]

    fmt = args.fmt
    today = date.today().strftime('%Y%m%d')

    if fmt == "quick":
        event_name = f"QuickDraftEmblem_{SET_CODE.upper()}_{today}"
    elif fmt == "premier":
        event_name = f"Draft_{SET_CODE.upper()}_{today}"
    elif fmt == "traditional":
        event_name = f"Trad_Draft_{SET_CODE.upper()}_{today}"
    else:  # autopick
        event_name = f"Draft_{SET_CODE.upper()}_{today}"

    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    LOG_PATH.touch()
    print(f"Log file: {LOG_PATH.resolve()}")
    print(f"Event:    {event_name}  format={fmt}")
    print()
    input("Press Enter to start the draft...")

    # Emit start event
    if fmt == "quick":
        # Quick Draft start: BotDraft_DraftStatus line
        payload = json.dumps({"EventName": event_name, "DraftStatus": "Idle"})
        request = json.dumps({"Payload": payload})
        outer = json.dumps({"request": request})
        write(f"[UnityCrossThreadLogger]==> BotDraft_DraftStatus {outer}")
    elif fmt in ("premier", "traditional", "autopick"):
        emit_premier_start(event_name)

    pack_idx = 0
    pick_num = 0

    # Emit first pack
    if fmt == "quick":
        emit_pack(pack_states[0], pack_num=0, pick_num=0, event_name=event_name)
    elif fmt in ("premier", "traditional"):
        emit_shape_a(pack_states[0], pack_num=0, pick_num=0)
    elif fmt == "autopick":
        # autopick: emit first shape D immediately (auto-picks first card)
        pass  # handled in loop

    total_picks = sum(len(p) for p in packs)
    picks_made = 0
    print(f"\n{total_picks} picks total. Press Enter to pick + see next pack (Ctrl+C to quit).\n")

    while True:
        try:
            input()
        except (KeyboardInterrupt, EOFError):
            print("\nStopped.")
            break

        current_pack = pack_states[pack_idx]
        picked = current_pack.pop(0)
        picks_made += 1
        pick_num += 1

        if fmt == "quick":
            emit_pick(picked)
        elif fmt == "premier":
            emit_pick_v2(picked)
        elif fmt == "traditional":
            emit_pick_v1(picked)
        # autopick: pick is embedded in shape D — no separate pick line

        if not current_pack:
            pack_idx += 1
            pick_num = 0
            if pack_idx >= len(packs):
                print("Draft complete.")
                break

        # Emit next pack
        if fmt == "quick":
            emit_pack(pack_states[pack_idx], pack_num=pack_idx, pick_num=pick_num, event_name=event_name)
        elif fmt in ("premier", "traditional"):
            if pack_idx == 0 and pick_num == 0:
                emit_shape_a(pack_states[pack_idx], pack_num=pack_idx, pick_num=pick_num)
            else:
                emit_shape_b(pack_states[pack_idx], pack_num=pack_idx, pick_num=pick_num)
        elif fmt == "autopick":
            next_pack = pack_states[pack_idx]
            next_picked = next_pack[0]  # preview — actual pop happens next loop
            emit_shape_d(list(next_pack), next_picked, event_name, pack_idx, pick_num)


if __name__ == "__main__":
    main()
