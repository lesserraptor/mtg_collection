# Pick Two Draft Detection — Implementation Plan

## Problem

The draft scanner in `src/draft/log_scanner.py` only detects draft format from the
`Event_Join` (underscore) log line prefix. Pick Two Draft uses `EventJoin` (no
underscore), so `state.format` is never set and the UI always falls back to
"PremierDraft".

## Log Evidence

**Draft start (no underscore):**
```
[UnityCrossThreadLogger]==> EventJoin {"EventName":"PickTwoDraft_SOS_20260421",...}
```

**Packs via Draft.Notify** (already handled by existing code, Shape B):
```
[UnityCrossThreadLogger]Draft.Notify {"draftId":"...","SelfPack":1,"SelfPick":1,
  "PackCards":"102549,102621,102520,..."}
```

**Picks (array — two cards per pick):**
```
[UnityCrossThreadLogger]==> EventPlayerDraftMakePick {
  "DraftId":"...",
  "GrpIds":[102534,102554],   ← both cards picked, not a single GrpId
  "Pack":2,"Pick":5
}
```

**Draft complete:**
```
[UnityCrossThreadLogger]==> DraftCompleteDraft {
  "EventName":"PickTwoDraft_SOS_20260421",
  "IsBotDraft":false
}
```

## Changes

### 1. `src/draft/log_scanner.py`

- Add `EVENT_JOIN_PREFIX = "[UnityCrossThreadLogger]==> EventJoin "` — no underscore
- Add `"PickTwoDraft": "PickTwoDraft"` to `_DRAFT_FORMAT_MAP`
- Add detection block for `EVENT_JOIN_PREFIX` in `_process_line()`:
  parse `EventName` from `request.EventName`, extract set code + map format
  (same logic as existing `Event_Join` block)
- Fix pick handling: `EventPlayerDraftMakePick` sends `GrpIds` (array of two IDs
  for Pick Two). Change from `int(payload["GrpId"])` to iterate over
  `payload.get("GrpIds", [payload.get("GrpId")])` — handles both single and
  double pick shapes.
- Add `DraftCompleteDraft` detection block: extract `EventName` → set code +
  format as a fallback for drafts that start without an explicit `EventJoin`
  line (e.g. if log starts mid-draft)

### 2. `src/web/routes/draft.py`

- Add `"PickTwoDraft"` to `ALLOWED_FORMATS` tuple
- `_sidebar_context()` picks up the new format automatically via the tuple

### 3. `src/web/templates/partials/draft_sidebar.html`

- Already loops over `ALLOWED_FORMATS` — no structural changes needed
- Buttons display the format key directly (e.g. "PickTwoDraft")
- Consider adding a short label hint in the sidebar above the buttons?
  (e.g. "PremierDraft, QuickDraft, TradDraft, PickTwoDraft")

## 17lands Integration

17lands supports `PickTwoDraft` as a first-class format value on the card_ratings
endpoint (`?expansion=SOS&format=PickTwoDraft`). No changes needed to
`seventeen_lands.py` — the format string flows through correctly once
`state.format` is set.

## Pick Data Shape

| Draft Type      | Pick Field | Shape            |
|-----------------|-----------|------------------|
| Premier/Trad    | `GrpId`   | single int       |
| Quick Draft     | nested    | `PickInfo.CardId` |
| Pick Two Draft  | `GrpIds`  | array of 2 ints  |

The fix iterates over `GrpIds` array, defaulting to single `GrpId` for backwards
compatibility.

## Bug Fix (May 2025)

When starting the scanner via "Start Listening" button in the UI, the set and
format were not detected on the first page render, showing "not detected" and
"PremierDraft" instead.

**Root cause**: `src/web/routes/draft.py` pre-set `state.file_offset` to the end
of the log file before calling `start_draft_scanner()`. This caused the initial
sync read to read from the file's end (finding no new content) instead of from
the beginning.

**Fix**: Removed the line that pre-set `file_offset` to file size. The scanner
now starts from offset 0, correctly picking up any existing log content.

Also in `src/draft/log_scanner.py`: added synchronous `_process_log_file()` to
do an initial read before returning from `start_draft_scanner()`, ensuring state
is updated before the UI redirect renders.
