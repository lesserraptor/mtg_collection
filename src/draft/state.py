from dataclasses import dataclass, field
from enum import Enum, auto


class DraftPhase(Enum):
    IDLE = auto()
    DRAFT_STARTED = auto()
    PACK_OFFERED = auto()
    PICK_MADE = auto()
    DRAFT_COMPLETE = auto()


@dataclass
class DraftState:
    phase: DraftPhase = DraftPhase.IDLE
    set_code: str = ""          # e.g. "MKM"
    format: str = ""            # e.g. "PremierDraft"
    pack_num: int = 0
    pick_num: int = 0
    pack_cards: list[int] = field(default_factory=list)   # arena_ids in current pack
    taken_cards: list[int] = field(default_factory=list)  # arena_ids picked so far
    ratings: dict[int, dict[str, float | None]] = field(default_factory=dict)  # {arena_id: {field: value}}
    file_offset: int = 0        # byte offset into Player.log
