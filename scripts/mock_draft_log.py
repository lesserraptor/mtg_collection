"""Write fake MTGA draft log events to data/Player.log for end-to-end testing.

Emits realistic draft log lines covering all supported MTGA draft formats.

Usage:
    .venv/bin/python scripts/mock_draft_log.py [set_code] [--format quick|premier|traditional|autopick|picktwo]

    e.g.  .venv/bin/python scripts/mock_draft_log.py fin
          .venv/bin/python scripts/mock_draft_log.py mkm --format premier
          .venv/bin/python scripts/mock_draft_log.py fin --format picktwo

Formats:
  quick        — Quick Draft / BotDraft (default): Shape C packs + BotDraft_DraftPick lines
  premier      — Premier Draft: Event_Join start + Shape A P1P1 + Shape B subsequent + V2 pick
  traditional  — Traditional Draft: Event_Join start + Shape A P1P1 + Shape B subsequent + V1 pick
  autopick     — Auto-pick simulation: Event_Join start + Shape D (LogBusinessEvents) combined lines
  picktwo      — Pick Two Draft: EventJoin start + Shape B packs + GrpIds array picks (2 per pack)

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
    choices=["quick", "premier", "traditional", "autopick", "picktwo"],
    default="quick",
    help="Draft format to simulate (default: quick)",
)
parser.add_argument(
    "--auto",
    action="store_true",
    help="Run automatically without waiting for Enter (for CI/testing)",
)
args = parser.parse_args()
SET_CODE = args.set_code.lower()
AUTO_MODE = args.auto


def get_cards_by_rarity(set_code: str) -> dict[str, list[int]]:
    db = sqlite3.connect(str(DB_PATH))
    rows = db.execute(
        "SELECT arena_id, rarity FROM cards WHERE set_code = ? AND rarity IN ('mythic', 'rare', 'uncommon', 'common')",
        (set_code,),
    ).fetchall()
    db.close()
    by_rarity: dict[str, list[int]] = {"mythic": [], "rare": [], "uncommon": [], "common": []}
    for arena_id, rarity in rows:
        r = rarity.lower()
        if r in by_rarity:
            by_rarity[r].append(arena_id)
    for r in by_rarity:
        if not by_rarity[r]:
            print(f"Warning: no {r} cards found for set '{set_code}'", file=sys.stderr)
    return by_rarity


def build_packs(by_rarity: dict[str, list[int]]) -> list[list[int]]:
    """Build 3 packs, each with 1 mythic, 1 rare, 3 uncommons, 9 commons."""
    packs = []
    for _ in range(3):
        pack = []
        # 1 mythic
        if by_rarity["mythic"]:
            pack.append(random.choice(by_rarity["mythic"]))
        # 1 rare
        if by_rarity["rare"]:
            pack.append(random.choice(by_rarity["rare"]))
        # 3 uncommons
        for _ in range(3):
            if by_rarity["uncommon"]:
                pack.append(random.choice(by_rarity["uncommon"]))
        # 9 commons
        for _ in range(9):
            if by_rarity["common"]:
                pack.append(random.choice(by_rarity["common"]))
        # shuffle within pack
        random.shuffle(pack)
        packs.append(pack)
    return packs


def write(line: str):
    with open(LOG_PATH, "a", encoding="utf-8") as f:
        f.write(line + "\n")


# ---------------------------------------------------------------------------
# Quick Draft emit functions (Shape C + BotDraft pick)
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
# Pick Two Draft emit functions
# ---------------------------------------------------------------------------

def emit_picktwo_start(event_name: str):
    """Emit EventJoin start line for Pick Two Draft."""
    request = json.dumps({"EventName": event_name, "EntryCurrencyType": "Gold", "EntryCurrencyPaid": 6000})
    outer = json.dumps({"request": request})
    write(f"[UnityCrossThreadLogger]==> EventJoin {outer}")


def emit_picktwo_picks(ids: list[int], pack: int, pick: int):
    """Emit EventPlayerDraftMakePick with GrpIds array for Pick Two (two cards per pick)."""
    request = json.dumps({"DraftId": "mock-draft-id", "GrpIds": ids, "Pack": pack, "Pick": pick})
    outer = json.dumps({"request": request})
    write(f"[UnityCrossThreadLogger]==> EventPlayerDraftMakePick {outer}")


def emit_picktwo_complete(event_name: str):
    """Emit DraftCompleteDraft completion line for Pick Two Draft."""
    request = json.dumps({"EventName": event_name, "IsBotDraft": False})
    outer = json.dumps({"request": request})
    write(f"[UnityCrossThreadLogger]==> DraftCompleteDraft {outer}")


# ---------------------------------------------------------------------------
# Main dispatcher
# ---------------------------------------------------------------------------

def main():
    by_rarity = get_cards_by_rarity(SET_CODE)
    total = sum(len(v) for v in by_rarity.values())
    print(f"Loaded {total} {SET_CODE.upper()} cards from DB")
    for r, ids in by_rarity.items():
        print(f"  {r}: {len(ids)}")

    packs = build_packs(by_rarity)
    # Deep copy for mutation
    pack_states = [list(p) for p in packs]

    fmt = args.fmt
    today = date.today().strftime('%Y%m%d')

    if fmt == "quick":
        event_name = f"QuickDraftEmblem_{SET_CODE.upper()}_{today}"
    elif fmt == "premier":
        event_name = f"Draft_{SET_CODE.upper()}_{today}"
    elif fmt == "traditional":
        event_name = f"Trad_Draft_{SET_CODE.upper()}_{today}"
    elif fmt == "autopick":
        event_name = f"Draft_{SET_CODE.upper()}_{today}"
    elif fmt == "picktwo":
        event_name = f"PickTwoDraft_{SET_CODE.upper()}_{today}"

    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    LOG_PATH.touch()
    print(f"Log file: {LOG_PATH.resolve()}")
    print(f"Event:    {event_name}  format={fmt}")
    print()

    if not AUTO_MODE:
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
    elif fmt == "picktwo":
        emit_picktwo_start(event_name)

    # Round-robin pick order: P1P1, P2P1, P3P1, P1P2, P2P2, P3P2, ...
    # We'll iterate through packs in round-robin fashion
    total_picks = sum(len(p) for p in packs)
    picks_made = 0

    # Track pick index per pack
    pick_indices = [0, 0, 0]

    if fmt == "picktwo":
        # Pick Two: 2 cards per pick, 7 picks per pack
        total_picks = 7 * 3

    print(f"\n{total_picks} picks total. ", end="")
    if AUTO_MODE:
        print("Auto mode — running all picks...")
    else:
        print("Press Enter for each pick (Ctrl+C to quit).")
    print()

    # Emit first round of packs (P1P1, P2P1, P3P1)
    for pack_idx in range(3):
        if fmt == "quick":
            emit_pack(pack_states[pack_idx], pack_num=pack_idx, pick_num=0, event_name=event_name)
        elif fmt in ("premier", "traditional"):
            if pack_idx == 0:
                emit_shape_a(pack_states[pack_idx], pack_num=pack_idx, pick_num=0)
            else:
                emit_shape_b(pack_states[pack_idx], pack_num=pack_idx, pick_num=0)
        elif fmt == "autopick":
            # autopick emits combined in loop
            pass
        elif fmt == "picktwo":
            emit_shape_b(pack_states[pack_idx], pack_num=pack_idx, pick_num=0)
            print(f"  pack {pack_idx + 1} pick 1 → {len(pack_states[pack_idx])} cards offered (Shape B)")

    round_num = 0
    while picks_made < total_picks:
        if not AUTO_MODE:
            try:
                input()
            except (KeyboardInterrupt, EOFError):
                print("\nStopped.")
                break

        # Round-robin: pack 0, 1, 2, 0, 1, 2, ...
        pack_idx = round_num % 3
        pick_idx = pick_indices[pack_idx]

        current_pack = pack_states[pack_idx]

        if fmt == "picktwo":
            # Pick Two: pop two cards at once
            if len(current_pack) < 2:
                round_num += 1
                continue
            picked_a = current_pack.pop(0)
            picked_b = current_pack.pop(0)
            picks_made += 1
            pick_indices[pack_idx] += 1
            emit_picktwo_picks([picked_a, picked_b], pack=pack_idx, pick=pick_idx)
            print(f"  P{pack_idx + 1} P{pick_idx + 1}: picked {picked_a}, {picked_b}")
        else:
            if not current_pack:
                round_num += 1
                continue
            picked = current_pack.pop(0)
            picks_made += 1
            pick_indices[pack_idx] += 1

            if fmt == "quick":
                emit_pick(picked)
            elif fmt == "premier":
                emit_pick_v2(picked)
            elif fmt == "traditional":
                emit_pick_v1(picked)
            # autopick: pick is embedded in shape D — no separate pick line

            print(f"  P{pack_idx + 1} P{pick_idx + 1}: picked {picked}")

        # Emit next pack for this pack_idx if cards remain
        if current_pack:
            next_pick_num = pick_indices[pack_idx]
            if fmt == "quick":
                emit_pack(current_pack, pack_num=pack_idx, pick_num=next_pick_num, event_name=event_name)
            elif fmt in ("premier", "traditional"):
                emit_shape_b(current_pack, pack_num=pack_idx, pick_num=next_pick_num)
            elif fmt == "autopick":
                next_picked = current_pack[0]
                emit_shape_d(list(current_pack), next_picked, event_name, pack_idx, next_pick_num)
            elif fmt == "picktwo":
                emit_shape_b(current_pack, pack_num=pack_idx, pick_num=next_pick_num)
                print(f"  pack {pack_idx + 1} pick {next_pick_num + 1} → {len(current_pack)} cards offered")
        else:
            # This pack exhausted
            pass

        round_num += 1

    if fmt == "picktwo":
        emit_picktwo_complete(event_name)
    print("Draft complete.")


if __name__ == "__main__":
    main()
