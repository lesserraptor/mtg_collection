# MTGA Collection Browser — Data Flows

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                            DATA SOURCES                                      │
│                                                                             │
│  ┌──────────────┐  ┌───────────────┐  ┌────────────┐  ┌──────────────────┐ │
│  │ MTGA CardDB   │  │ Scryfall Bulk │  │ Collection │  │   Player.log     │ │
│  │ .mtga SQLite  │  │ ~2GB JSON     │  │ x.json     │  │  (game log)      │ │
│  │ (native game) │  │ (API)         │  │ (companion) │  │                  │ │
│  └──────┬───────┘  └───────┬───────┘  └─────┬──────┘  └────────┬─────────┘ │
│         │                  │                │                   │           │
│         ▼                  ▼                ▼                   ▼           │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │                    INGEST PIPELINE (ingest_cli.py)                    │  │
│  │                                                                       │  │
│  │  [1/6] DB Init ──► [2/6] CardDB Load ──► [3/6] Scryfall Enrich       │  │
│  │         │                  │                       │                  │  │
│  │         ▼                  ▼                       ▼                  │  │
│  │  schema + indexes    cards table seeded       cards enriched          │  │
│  │                      (arena_id, name,         (scryfall_id, image,    │  │
│  │                       mana, rarity,            type_line, oracle,     │  │
│  │                       set_code, cn)            cmc, colors, etc.)     │  │
│  │                                                                       │  │
│  │  [4/6] Collection ──► [5/6] Wallet ──► [6/6] Errata                   │  │
│  │         │                  │                  │                        │  │
│  │         ▼                  ▼                  ▼                        │  │
│  │  collection table    wallet_snapshots    basic land fixes +           │  │
│  │  + snapshots + diffs                     KNOWN_ERRATA applied         │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
│                                      │                                       │
│                                      ▼                                       │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │              SQLite DB: data/mtga_collection.db                      │  │
│  │                                                                       │  │
│  │  cards ─► collection ─► collection_snapshots ─► collection_snapshot_diffs │
│  │  decks ─► deck_lines ─► deck_versions ─► deck_version_lines           │  │
│  │  wallet_snapshots ─► draft_results ─► set_info                        │  │
│  │  errata ─► meta ─► mastery_pass_purchases                             │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
│                                      │                                       │
│                                      ▼                                       │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │              WEB UI (FastAPI + HTMX + Jinja2)                       │  │
│  │                                                                       │  │
│  │  /cards          Deck builder / card catalog browser                 │  │
│  │  /decks          Deck list viewer                                    │  │
│  │  /draft          Live draft picker + 17lands ratings                 │  │
│  │  /sets           Set tracker                                         │  │
│  │  /analysis       Collection analytics                                │  │
│  │  /changes        Collection change history                           │  │
│  │  /settings       Configuration UI                                    │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
│                                                                             │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │              FILE WATCHERS (auto-reload on change)                   │  │
│  │                                                                       │  │
│  │  Watchdog Observer ──► OS thread ──► call_soon_threadsafe ──►        │  │
│  │    asyncio.Queue ──► debounce (2s) ──► upsert_collection()           │  │
│  │                                                                       │  │
│  │  Same pattern for Player.log changes ──► wallet snapshot + draft     │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 1. Card Catalog Flow (Native MTGA CardDB + Scryfall Enrichment)

**Source 1 — Native CardDB**

| Aspect | Detail |
|---|---|
| File | `src/db/mtga_card_db.py` |
| Input | `Raw_CardDatabase_*.mtga` (SQLite shipped with MTGA game client) |
| Discovery | `find_card_db()` — checks meta table for saved path, then falls through 4 glob patterns |

**Path resolution order:**
1. `$MTGA_CARD_DB_PATH` env var override
2. `data/Raw_CardDatabase.mtga` (project-local, portable)
3. Linux/Proton: `~/.local/share/Steam/steamapps/common/MTGA/MTGA_Data/Downloads/Raw/Raw_CardDatabase_*.mtga`
4. macOS: `~/Library/Application Support/com.wizards.mtga/Downloads/Raw/Raw_CardDatabase_*.mtga`

**Extraction query:**
```sql
SELECT c.GrpId AS arena_id,
       l.Loc   AS name,
       c.ExpansionCode AS set_code,
       c.CollectorNumber AS collector_number,
       c.Rarity AS rarity_id,
       c.IsRebalanced AS is_rebalanced,
       c.OldSchoolManaText AS raw_mana,
       c.Types AS type_ids,
       c.Colors AS color_ids
FROM Cards c
JOIN Localizations_enUS l ON c.TitleId = l.LocId AND l.Formatted = 1
WHERE c.IsPrimaryCard = 1
```

**Transformations:**
- **Name cleanup**: Regex strips HTML tags (`<[^>]+>`) from card names
- **Mana cost decode**: `_decode_mana()` converts MTGA's `OldSchoolManaText` format (e.g. `"o2oWoU"` → `"{2}{W}{U}"`) by segment-splitting on `'o'` prefix characters
- **Rarity map**: `{0: "token", 1: "basic", 2: "common", 3: "uncommon", 4: "rare", 5: "mythic"}`
- **Set code lowercased**, collector number stringified

**Storage:**
```sql
INSERT OR IGNORE INTO cards (
    arena_id, scryfall_id, name, mana_cost, cmc, type_line, oracle_text,
    rarity, set_code, collector_number, colors, color_identity, keywords,
    layout, image_uri_front, image_uri_back, is_rebalanced
) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
```

Fields left as `""` or `None` (for Scryfall to fill): `scryfall_id`, `cmc`, `type_line`, `oracle_text`, `colors`, `color_identity`, `keywords`, `layout`, `image_uri_front`, `image_uri_back`

**Key constraint**: `INSERT OR IGNORE` — native CardDB rows are never overwritten by Scryfall.

---

**Source 2 — Scryfall Bulk Enrichment**

| Aspect | Detail |
|---|---|
| File | `src/db/ingest.py` |
| Input | `data/scryfall_all_cards.json` (~2GB, stream-parsed) |
| API endpoint | `https://api.scryfall.com/bulk-data` (type: `all_cards`) |

**Download flow (`download_bulk()`):**
1. Fetch bulk index JSON from Scryfall
2. Find entry with `type == "all_cards"`
3. Stream-download via `requests.get(stream=True)` in 64KB chunks
4. Write to cache file with dot-progress every 10MB (stderr)

**Parse flow (`ingest_scryfall()`):**
- Stream-parsed with `ijson` (never loads full file into memory)
- Filters to English cards only (`card["lang"] == "en"`)
- Each card processed into two SQL tuples via `_card_to_rows()`:
  - **Enrich tuple** → `ENRICH_CARD_SQL` UPDATE (fills NULLs left by native CardDB)
  - **Insert tuple** → `INSERT_NEW_CARD_SQL` INSERT OR IGNORE (cards Scryfall knows that native doesn't)

**Enrichment SQL (COALESCE pattern):**
```sql
UPDATE cards SET
    scryfall_id      = ?,
    mana_cost        = ?,
    cmc              = COALESCE(cmc, ?),
    type_line        = COALESCE(type_line, ?),
    oracle_text      = ?,                    -- overwritten unconditionally
    rarity           = COALESCE(NULLIF(rarity,'common'), ?),
    set_code         = COALESCE(NULLIF(set_code,''), ?),
    collector_number = COALESCE(collector_number, ?),
    colors           = COALESCE(NULLIF(colors,'[]'), ?),
    color_identity   = COALESCE(NULLIF(color_identity,'[]'), ?),
    keywords         = COALESCE(NULLIF(keywords,'[]'), ?),
    layout           = COALESCE(layout, ?),
    image_uri_front  = ?,
    image_uri_back   = ?,
    booster          = ?
WHERE arena_id = ?
```

**Fields overwritten unconditionally**: `oracle_text`, `booster` — Scryfall is authoritative for these.

**Arena ID resolution (4 tiers)** — for Scryfall cards missing `arena_id`:
1. **(set, collector_number) map** — direct match, with basic land collision guard: rejects if both Scryfall name and DB name are basics but differ (off-by-one arena_id in Scryfall data)
2. **(card_name, set) map** — name + set match; skips borderless/frame-effect variants
3. **Unambiguous name map** — cards with only 1 Arena printing of that name
4. **Name-only match against unclaimed DB cards** — handles set-code mismatches (Y25 vs YTDM, SPG reprints, Final Fantasy collaborations) — matches any name_lower with exactly 1 unclaimed DB candidate still lacking `scryfall_id`

**Rebalanced (A-) handling:**
- Scryfall stores Alchemy rebalanced cards with `collector_number = "A-{N}"` and no `arena_id`
- `rebalanced_id_map` built from `cards WHERE is_rebalanced=1` linking `(set_code, collector_number)` → `arena_id`
- Bulk parser skips A- collector numbers that can't be matched

**Image URI extraction (`_extract_image_uris()`):**
- Top-level `card["image_uris"]` wins first (correct for adventure cards)
- DFC/modal_dfc/split/flip → `card_faces[0]` / `card_faces[1]`
- Uses `"normal"` size URI

**Oracle text for multi-face cards (`_extract_oracle_text()`):**
- Top-level `oracle_text` wins if present
- Split/adventure/flip/prepare layouts: join both faces with `"\n//\n"`
- DFC/transform/meld: front face only

---

**Source 2b — Scryfall API Fallback (opt-in only)**

| Aspect | Detail |
|---|---|
| File | `ingest.py: enrich_missing_from_api()` |
| Endpoint | `https://api.scryfall.com/cards/named?exact={NAME}` |

- NOT part of the default pipeline — only runs with `--api-enrich` in `ingest_cli.py`
- Fetches cards that bulk ingest missed (set code mismatches where even tier 4 failed)
- One card at a time with 100ms sleep between requests (rate limiting)
- Usually handles ~200–300 cards, ~100ms each
- Uses `ENRICH_CARD_SQL` same as bulk ingest

---

**Source 2c — Rebalanced Image Backfill**

| Aspect | Detail |
|---|---|
| File | `ingest.py: backfill_rebalanced_images()` |

```sql
UPDATE cards SET
    scryfall_id = (SELECT std.scryfall_id FROM cards std
                   WHERE std.set_code = cards.set_code
                     AND std.collector_number = cards.collector_number
                     AND std.is_rebalanced = 0 AND std.scryfall_id != ''
                   LIMIT 1),
    image_uri_front = (SELECT ... LIMIT 1),
    image_uri_back = (SELECT ... LIMIT 1)
WHERE is_rebalanced = 1 AND (scryfall_id = '' OR scryfall_id IS NULL)
```

Scryfall has no separate entries for Alchemy rebalanced variants — copies image data from the standard (non-rebalanced) card sharing the same set_code + collector_number.

---

**Pipeline Orchestration**

| File | `src/db/ingest_cli.py` |
|---|---|

**Phases:**
1. Database init — `init_db()` (schema + indexes)
2. Native CardDB load — `DELETE FROM cards` (fresh start) + `ingest_mtga_card_db()`
3. Scryfall enrichment — download (if needed) + `ingest_scryfall()` + `backfill_rebalanced_images()` + `populate_set_info_from_scryfall()` (+ `enrich_missing_from_api()` only with `--api-enrich`)
4. Collection upsert — `upsert_collection()`
5. Wallet snapshot — `capture_wallet_snapshot()`
6. Errata — `apply_errata()` + summary

**Fallback map construction (between phases 2 and 3):**
```python
# Build arena_id_map, name_set_map, name_map, src_name_by_id, rebalanced_id_map
# from the freshly-loaded cards table so Scryfall parser can resolve arena_ids
# for cards missing arena_id in the bulk data
rows = conn.execute("SELECT arena_id, name, set_code, collector_number FROM cards")
```

**CLI flags:**
| Flag | Effect |
|---|---|
| `--skip-download` | Use existing cached Scryfall JSON |
| `--skip-mtga-card-db` | Skip native CardDB step (MTGA not installed) |
| `--skip-collection` | Skip collection upsert |
| `--skip-wallet` | Skip wallet snapshot |
| `--wallet-only` | Only capture wallet snapshot |

**Consumption:**
- All card catalog UI pages: `/cards`, `/decks`, `/draft`, `/changes`, `/analysis`
- Card detail modals, set tracker, mana cost rendering

---

## 2. Collection Data Flow

| File | `src/collection.py` |
|---|---|

**Source path resolution (`find_collection_file()`):**
1. Meta table key `default_collection_path`
2. `~/.local/share/Steam/steamapps/compatdata/2141910/pfx/drive_c/x.json`
3. `./x.json`

**Supported input formats:**

**JSON format** (MTGA companion app output):
```json
{"cards": [{"grpid": 12345, "quantity": 4}, ...]}
```
Parsed as: `[(int(c["grpid"]), int(c["quantity"]), now) for c in data["cards"]]`

**SQLite format** (legacy mtga_collection.db):
```sql
SELECT arena_id, quantity FROM cards
```

**Upsert logic:**
```sql
INSERT OR REPLACE INTO collection (arena_id, quantity, updated_at) VALUES (?,?,?)
```

**Snapshot + diff tracking:**
1. `_snapshot_collection(conn)` — captures current state as `{arena_id: quantity}` dict
2. After upsert, captures new snapshot
3. `_persist_diff()` — computes per-card diff, inserts into `collection_snapshot_diffs`
4. Creates `collection_snapshots` row with timestamp + source tag

**Snapshot diff schema:**
```sql
CREATE TABLE collection_snapshot_diffs (
    snapshot_id  INTEGER NOT NULL REFERENCES collection_snapshots(id),
    arena_id     INTEGER NOT NULL,
    card_name    TEXT,
    old_quantity INTEGER NOT NULL DEFAULT 0,
    new_quantity INTEGER NOT NULL DEFAULT 0,
    diff         INTEGER NOT NULL,
    PRIMARY KEY (snapshot_id, arena_id)
);
```

**Audited vs Non-Audited:**
- **Audited** (authoritative): quantities from collection JSON / companion app SQLite — these are the actual counts of cards owned in MTGA
- **Non-Audited** (estimates): CardDB-only entries with `quantity = 0` in collection table — these are cards known to exist in the game but not yet confirmed as owned
- Collection upsert always overwrites (INSERT OR REPLACE), so audited data always wins

**Auto-reload via file watcher:**

```
┌──────────────────────────────────────────────────────────────────────┐
│ Watchdog Observer (OS thread)                                        │
│                                                                      │
│  CollectionFileHandler.on_modified(event)                            │
│    └─ loop.call_soon_threadsafe(queue.put_nowait, path)             │
│                                                                      │
│  ┌────────────────────────────────────────────────────────────────┐  │
│  │  Async Consumer (asyncio thread)                                │  │
│  │                                                                  │  │
│  │  collection_reload_consumer(app, queue):                         │  │
│  │    1. await queue.get()                                          │  │
│  │    2. asyncio.sleep(2.0)  ← debounce MTGA multi-flush          │  │
│  │    3. Drain any remaining queued events (get_nowait)             │  │
│  │    4. find_collection_file()                                     │  │
│  │    5. upsert_collection(app.state.db, collection_path)           │  │
│  └────────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────┘
```

**Consumption:**
- `/cards` — shows owned quantities, filters for owned/missing cards
- `/analysis` — collection completeness statistics
- `/changes` — per-card quantity change history
- `/draft` — owned_quantity shown in pack overlay

---

## 3. Deck Data Flow

| File | `src/db/log_parser.py` |
|---|---|

**Source:** `Player.log` from MTGA installation

**Path resolution (`find_player_log()`):**
1. Meta table key `default_log_path`
2. `$MTGA_LOG_PATH` env var
3. Linux/Proton: `~/.local/share/Steam/steamapps/compatdata/2141910/pfx/drive_c/users/steamuser/AppData/LocalLow/Wizards Of The Coast/MTGA/Player.log`
4. macOS: `~/Library/Logs/Wizards Of The Coast/MTGA/Player.log`

**Primary parser — StartHook event:**
- Scans log in reverse for most recent `StartHook` JSON payload (contains `"Decks"` + `"InventoryInfo"`)
- Extracts all decks from `Decks` dict: each entry has `MainDeck`, `Sideboard`, `CommandZone`
- Enriches with names/format from `DeckSummariesV2` payload
- Skips precon decks (`?=?Loc/Decks/Precon/...`)

**Deck schema:**
```python
{
    "deck_id": str,
    "name": str,
    "format": str,     # "Standard", "Limited", etc.
    "mainboard": [{"arena_id": int, "quantity": int}, ...],
    "sideboard": [{"arena_id": int, "quantity": int}, ...],
    "commander": [{"arena_id": int, "quantity": int}, ...],
}
```

**Fallback parser — DeckUpsertDeckV2 events:**
- Scans for `DeckUpsertDeckV2` markers + subsequent JSON lines with `"MainDeck"`
- Deduplicates by `Id`

**Storage tables:**
```sql
decks (id, name, format, imported_at, source, is_potential, is_saved, log_deck_id, content_hash, scan_status)
deck_lines (id, deck_id, arena_id, card_name, quantity, section)
deck_versions (id, deck_id, version_num, label, created_at, source)
deck_version_lines (id, version_id, arena_id, card_name, quantity, section)
```

**Deck detection semantics:**
- StartHook returns ALL current decks visible in MTGA, not just saved ones
- `is_potential = 1` marks unsaved/AI-created/brawl decks
- `is_saved = 1` marks user-saved decks
- `scan_status` tracks `'active'` vs `'removed'` for decks that disappear between log scans
- Each scan creates a new version in `deck_versions` + `deck_version_lines` for historical tracking

**Consumption:**
- `/decks` — deck list with card counts, format filters
- `/cards` — deck demand shown in card browser
- `/draft` — deck demand metrics in pack overlay

---

## 4. Draft + 17lands Data Flow

### Live Draft Scanning (`src/draft/log_scanner.py` + `src/draft/state.py`)

**Architecture (mirrors watcher.py):**
```
┌──────────────────────────────────────────────────────────────────────┐
│ Watchdog PollingObserver (OS thread)                                 │
│                                                                      │
│  LogScanner.on_modified(event)                                       │
│    └─ loop.call_soon_threadsafe(queue.put_nowait, path)             │
│                                                                      │
│  ┌────────────────────────────────────────────────────────────────┐  │
│  │  Asyncio Consumer                                              │  │
│  │                                                                │  │
│  │  log_consumer(app, queue, state):                              │  │
│  │    1. Read new bytes from file offset                          │  │
│  │    2. _process_line() per new line                             │  │
│  │    3. Fetch 17lands ratings if needed                          │  │
│  │    4. render_pack_html() → push to SSE queue                   │  │
│  └────────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────┘
```

**DraftState (`src/draft/state.py`):**
```python
@dataclass
class DraftState:
    phase: DraftPhase         # IDLE → DRAFT_STARTED → PACK_OFFERED → PICK_MADE → DRAFT_COMPLETE
    set_code: str             # e.g. "MKM"
    format: str               # e.g. "PremierDraft"
    pack_num: int
    pick_num: int
    pack_cards: list[int]     # arena_ids in current pack
    taken_cards: list[int]    # arena_ids picked so far
    ratings: dict             # {arena_id: {field: value}} from 17lands
    file_offset: int          # byte offset in Player.log
    entry_cost_gold: int
    wins: int
```

**Log event parsing (`_process_line()`):**

| Event | Prefix / Detection | Extracted Data |
|---|---|---|
| **Draft start** | `Event_Join`, `EventJoin`, `BotDraft_DraftStatus` | Set code, format, entry cost gold |
| **Pack offered** (Shape A) | `CardsInPack` (P1P1) | Card arena_ids, pack number, pick number |
| **Pack offered** (Shape B) | `Draft.Notify` (post-P1P1) | Card arena_ids, SelfPack, SelfPick |
| **Pack offered** (Shape C) | BotDraft `DraftStatus=PickNext` + `DraftPack` | Card IDs, pack/pick number, PickedCards, EventName |
| **Pick made** (V1) | `Event_PlayerDraftMakePick` | GrpIds from payload |
| **Pick made** (V1B) | `EventPlayerDraftMakePick` | GrpIds from request or payload |
| **Pick made** (V2) | `Draft.MakeHumanDraftPick` | cardId |
| **Pick made** (Bot) | `BotDraft_DraftPick` | PickInfo.CardId |
| **Auto-pick** (Shape D) | LogBusinessEvents + PickGrpId + CardsInPack | Combined pack + pick |
| **Draft complete** | `Draft_CompleteDraft`, `DraftCompleteDraft` | None (just phase change) |
| **Prize claim** | `EventClaimPrize` | CurrentWins → reward lookup |
| **Prize response** | InternalEventName + InventoryInfo in same line | Real wins, losses, gem changes |
| **Match win** | `matchGameRoomStateChangedEvent` | winningTeamId → state.wins++ |

**Format mapping:**
| Draft type | Format string |
|---|---|
| Premier | `"PremierDraft"` |
| Quick/Bot | `"QuickDraft"` |
| Traditional | `"TradDraft"` |
| Pick Two | `"PickTwoDraft"` |
| Sealed | `"Sealed"`, `"TradSealed"` |

**Alchemy set code stripping:**
```python
# Strips Y25, Y, A prefixes from set codes before querying 17lands
# e.g. "Y25TDM" → "TDM"
for prefix in ("Y25", "Y", "A"):
    if code.startswith(prefix) and 2 <= len(code[len(prefix):]) <= 4:
        return code[len(prefix):].upper()
```

---

### 17lands Integration (`src/draft/seventeen_lands.py`)

| Aspect | Detail |
|---|---|
| Endpoint | `https://www.17lands.com/card_ratings/data` |
| Params | `expansion={set_code}`, `format={format_str}` |
| Method | Async HTTP via `httpx.AsyncClient` |
| Cache | Module-level `_RATINGS_CACHE: dict[(set_code, format), dict]` |

**Metrics fetched:**
| Metric | Field | Threshold | Higher better |
|---|---|---|---|
| GIHWR | `ever_drawn_win_rate` | 500 games | Yes |
| OHWR | `opening_hand_win_rate` | 500 games | Yes |
| IWD | `drawn_improvement_win_rate` | 500 games | Yes |
| GWR | `win_rate` | 500 games | Yes |
| ATA | `avg_pick` | 200 picks | No |
| ALSA | `avg_seen` | 200 picks | No |

Values below threshold → stored as `None`.

**Cache behavior:**
- Keyed by `(set_code.upper(), format_str)`
- Fetched once per draft session (cleared when new draft starts)
- Cache survives across picks within same draft

---

### Server-Side Rendering (`render_pack_html()`)

**Query for pack card data:**
```sql
WITH deck_demand AS (
    SELECT LOWER(CASE WHEN instr(dl.card_name, ' // ') > 0
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
```

**Sort order:** mythic → rare → uncommon → common (by rarity ordinal)

**Top picks:** Top 3 by selected metric (default GIHWR), filtered by active color filter

**Delivery:** SSE (Server-Sent Events) via `app.state.draft_event_queue`

**Consumption:**
- `/draft` — live picker UI with card images, ratings overlay, owned quantity, deck demand

---

## 5. Wallet / Resource Data Flow

| File | `src/wallet.py` + `src/db/log_parser.py` |
|---|---|

**Source:** StartHook event in Player.log (same parse as decks)

**Extraction (`_parse_wallet_from_starthook()`):**
```python
{
    "gems": inv.get("Gems", 0),
    "gold": inv.get("Gold", 0),
    "mythic_wc": inv.get("WildCardMythics", 0),
    "rare_wc": inv.get("WildCardRares", 0),
    "uncommon_wc": inv.get("WildCardUnCommons", 0),
    "common_wc": inv.get("WildCardCommons", 0),
    "draft_tokens": inv.get("CustomTokens", {}).get("DraftToken", 0),
}
```

**Total cards computed:**
```sql
SELECT COUNT(DISTINCT LOWER(c.name))
FROM cards c
JOIN collection col ON c.arena_id = col.arena_id
WHERE col.quantity > 0
```

**Storage:**
```sql
INSERT OR REPLACE INTO wallet_snapshots
(date, gems, gold, mythic_wc, rare_wc, uncommon_wc, common_wc, draft_tokens, total_cards)
VALUES (?,?,?,?,?,?,?,?,?)
```

Per-day snapshot — `INSERT OR REPLACE` keeps only latest value per day.

**Triggered by:**
1. CLI phase 5: `capture_wallet_snapshot(conn)`
2. File watcher: `WalletFileHandler` watches Player.log directory → `wallet_reload_consumer()` → `capture_wallet_snapshot_from_log()`
3. Standalone: `python src/db/ingest_cli.py --wallet-only`

**Consumption:**
- `/analysis` — wallet/resource trend graphs over time
- Settings panel — current resource display

---

## 6. Draft Results Flow

| File | `src/draft/log_scanner.py` (prize claim handlers) |
|---|---|

**Sources:**

**Source A — EventClaimPrize event:**
```
[UnityCrossThreadLogger]==> EventClaimPrize {request: {...}}
```
Extracts `CurrentWins`, calculates gem reward from `REWARD_LOOKUP` table per format, computes trophy status.

**Source B — Prize claim response JSON:**
Detected by presence of both `"InternalEventName"` and `"InventoryInfo"` on one line (the response after the `<=` header). Contains real wins, losses, gem changes.

**Reward lookup tables:**
```python
REWARD_LOOKUP = {
    "PickTwoDraft":  {0: 50, 1: 150, 2: 800, 3: 1000, 4: 1300},
    "QuickDraft":    {0: 50, 1: 100, 2: 200, 3: 300, 4: 450, 5: 650, 6: 850, 7: 950},
    "PremierDraft":  {0: 50, 1: 100, 2: 250, 3: 1000, 4: 1400, 5: 1600, 6: 1800, 7: 2200},
    "TradDraft":     {0: 200, 1: 500, 2: 1200, 3: 1800, 4: 2200},
}
```

**Trophy determination:**
```python
trophy = 1 if (fmt == "PickTwoDraft" and wins >= 4) or (fmt != "PickTwoDraft" and wins >= 7) else 0
```

**Default entry costs (used when entry cost not captured):**
```python
_DEFAULT_COST = {
    "QuickDraft": 5000,
    "PremierDraft": 10000,
    "PickTwoDraft": 6000,
    "TradDraft": 10000,
}
```

**Guard:** Only inserts when `entry_cost_gold > 0` (prevents incomplete/spurious rows).

**Storage:**
```sql
INSERT INTO draft_results (date, set_code, format, wins, losses, cost_gold, winnings_gems, trophy)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
```

**Consumption:**
- `/draft` — results table below the live picker
- `/analysis` — draft win-rate stats, ROI calculations, trophy count

---

## 7. Errata / Data Correction Flow

| File | `src/db/errata.py` |
|---|---|

**Three strategies, applied in order:**

**Strategy 1 — Systematic basic-land fix (name-based):**
```python
BASIC_LAND_CORRECTIONS = [
    ("Plains",   "Basic Land — Plains",   '["W"]'),
    ("Island",   "Basic Land — Island",   '["U"]'),
    ("Swamp",    "Basic Land — Swamp",    '["B"]'),
    ("Mountain", "Basic Land — Mountain", '["R"]'),
    ("Forest",   "Basic Land — Forest",   '["G"]'),
]
```
```sql
UPDATE cards
SET type_line = ?, color_identity = ?
WHERE name = ?
  AND (type_line != ? OR color_identity != ?)
  AND type_line LIKE 'Basic Land%'
```
Fixes ~180 rows across 57 sets where Scryfall's off-by-one arena_id assignment caused wrong land type/color data.

**Strategy 2 — KNOWN_ERRATA seeding:**
- Hardcoded per-arena_id overrides for fields like `scryfall_id`, `image_uri_front`, `type_line`, `color_identity`
- Seeded into `errata` table via `INSERT OR REPLACE` on every `apply_errata()` call
- Currently covers ~12 rows (basic lands with wrong scryfall_id/image URIs)

```python
KNOWN_ERRATA = [
    (58441, "scryfall_id", "9c010ca5-...", "basic land scryfall_id fix"),
    (58441, "image_uri_front", "https://...", "basic land image fix"),
    # ... 11 more entries
]
```

**Strategy 3 — Table-driven:**
- Any additional rows manually inserted into `errata` table are applied as targeted UPDATEs
- User can insert one-off fixes at the SQLite level

**Allowed fields frozenset:**
```python
_ALLOWED_FIELDS = {
    "name", "mana_cost", "cmc", "type_line", "oracle_text", "rarity",
    "set_code", "collector_number", "colors", "color_identity", "keywords",
    "layout", "image_uri_front", "image_uri_back", "scryfall_id", "is_rebalanced",
}
```

**Applied at end of ingest pipeline (phase 6)**, ensuring corrections survive every data reload.

**Consumption:**
- All downstream consumers benefit transparently — errata patches the `cards` table directly
- `errata` table provides an audit trail of applied corrections

---

## 8. Set Info Flow

| File | `src/db/ingest.py` (populate) + `src/web/routes/_set_info.py` (cache) |
|---|---|

**Purpose:** Maps set codes to display names, SVG icons, and set types for the set tracker UI.

**Major set types (used for filters across pages):**
```python
MAJOR_SET_TYPES = frozenset({"expansion", "core", "masters", "draft_innovation"})
```

**Population (ingest pipeline):**

Set metadata is fetched once from `https://api.scryfall.com/sets` during the Scryfall enrichment step and written to the `set_info` table:

```
┌──────────────────────────────────────────────────────────────────┐
│  populate_set_info_from_scryfall(conn)  ← in src/db/ingest.py    │
│                                                                   │
│  1. GET https://api.scryfall.com/sets                            │
│     → 1042 sets returned, ~194 primary-type                      │
│  2. INSERT OR REPLACE INTO set_info                              │
│     (set_code, name, icon_svg_uri, set_type, updated_at)         │
│                                                                   │
│  Called after Scryfall bulk enrich in:                           │
│    • ingest_cli.py [3/6] — during full pipeline                  │
│    • settings.py /refresh-scryfall — manual refresh via UI       │
└──────────────────────────────────────────────────────────────────┘
```

**Caching (web layer):**

The web layer reads from the DB cache only — no live Scryfall calls:

```
┌──────────────────────────────────────────────────────────────────┐
│  _ensure_set_info(db)  ← in src/web/routes/_set_info.py          │
│                                                                   │
│  1. If _SET_INFO already populated → return                      │
│  2. SELECT set_code, name, icon_svg_uri, set_type FROM set_info  │
│     → _SET_INFO dict populated from DB                           │
│     → UI never shows blank if Scryfall is down                   │
│                                                                   │
│  No live Scryfall API call.                                      │
│  DB was populated during ingest.                                 │
└──────────────────────────────────────────────────────────────────┘
```

**Cache invalidation:**
- `invalidate_set_info_cache()` clears the in-memory `_SET_INFO` dict
- Called after `refresh-scryfall` so the new set data is picked up on the next page load

**DB cache schema:**
```sql
CREATE TABLE IF NOT EXISTS set_info (
    set_code     TEXT PRIMARY KEY,
    name         TEXT NOT NULL,
    icon_svg_uri TEXT NOT NULL DEFAULT '',
    set_type     TEXT NOT NULL DEFAULT '',
    updated_at   TEXT NOT NULL
);
```

**Consumption:**
- `/sets` — set tracker with checkboxes, icons, set type filter
- `/cards` — set dropdown filter (uses same `MAJOR_SET_TYPES` for default filtering)
- The module-level `_SET_INFO` singleton is shared across routes so there's never a double-fetch

---

## Source of Truth Summary

Which source wins for each field in the `cards` table:

| Field | Source of Truth | Why |
|---|---|---|
| `arena_id` | Native CardDB | MTGA's internal ID; Scryfall may be missing it |
| `name` | Native CardDB | English name from localization table; HTML tags stripped |
| `mana_cost` | Native CardDB | CardDB's `OldSchoolManaText` decoded; Scryfall enrichment can supplement for non-native cards via COALESCE |
| `rarity` | Native CardDB | Rarity IDs 0–5 mapped to strings; Scryfall only fills via COALESCE for non-native rows |
| `set_code` | Native CardDB | CardDB `ExpansionCode`; Scryfall only fills via COALESCE |
| `collector_number` | Native CardDB | CardDB value; Scryfall fills NULLs via COALESCE |
| `is_rebalanced` | Native CardDB | CardDB flag (0/1); Scryfall insert tuples hardcode 0 |
| `cmc` | Scryfall (fill) | Native CardDB leaves NULL; COALESCE means Scryfall fills |
| `type_line` | Scryfall (fill) | Native CardDB leaves NULL; COALESCE fills; exception: basic lands corrected by errata strategy 1 |
| `oracle_text` | Scryfall (unconditional) | Native CardDB leaves NULL; Scryfall overwrites unconditionally |
| `scryfall_id` | Scryfall (fill) | Native leaves `""`; Scryfall fills; basic land off-by-one fixed by errata |
| `colors` | Scryfall (fill) | Native leaves `"[]"`; Scryfall fills via COALESCE NULLIF |
| `color_identity` | Scryfall (fill) | Native leaves `"[]"`; Scryfall fills via COALESCE NULLIF; basic lands corrected by errata |
| `keywords` | Scryfall (fill) | Native leaves `"[]"`; Scryfall fills |
| `layout` | Scryfall (fill) | Native leaves NULL; Scryfall fills |
| `image_uri_front` | Scryfall (fill) | Native leaves NULL; Scryfall fills; basic lands fixed by errata; rebalanced backfilled from standard |
| `image_uri_back` | Scryfall (fill) | Native leaves NULL; Scryfall fills; rebalanced backfilled from standard |
| `booster` | Scryfall (unconditional) | Native CardDB has no equivalent; Scryfall overwrites |

**Other table source of truth:**

| Table | Source | Notes |
|---|---|---|
| `collection` | Collection JSON / companion SQLite | INSERT OR REPLACE — always overwritten |
| `decks` + `deck_lines` | Player.log StartHook | Each scan creates a new version |
| `collection_snapshots` | Collection upsert | Auto-generated diff tracking |
| `wallet_snapshots` | Player.log StartHook | Per-day snapshot |
| `draft_results` | Player.log prize events | Only inserted on completed drafts |
| `set_info` | Scryfall `/sets` API (during ingest only) | Populated by ingest pipeline, read by web layer |
| `errata` | Hardcoded + manual overrides | Seeded every pipeline run |
| `meta` | Various pipeline steps | Key-value config/last-updated store |

---

## File Watcher Architecture (Cross-Cutting)

All three file watchers follow the same pattern:

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    WATCHDOG PATTERN                                       │
│                                                                          │
│  OS Thread                     │  Asyncio Thread                         │
│                                │                                          │
│  FileSystemEventHandler        │  asyncio.Queue                          │
│    .on_modified(event) ───────►│                                          │
│      loop.call_soon_threadsafe │  Coroutine:                             │
│        (queue.put_nowait,path) │    1. await queue.get()                 │
│                                │    2. asyncio.sleep(2.0) — debounce     │
│                                │    3. Drain duplicate events            │
│                                │    4. DB operation                      │
└─────────────────────────────────────────────────────────────────────────┘
```

| Watcher | Handler | Consumer | DB Operation |
|---|---|---|---|
| Collection | `CollectionFileHandler` | `collection_reload_consumer` | `upsert_collection()` |
| Wallet | `WalletFileHandler` | `wallet_reload_consumer` | `capture_wallet_snapshot_from_log()` |
| Draft | `LogScanner` | `log_consumer` | `_process_line()` + insert draft results |

All DB access happens on the asyncio thread only — no cross-thread SQLite access. Watchdog OS thread never touches any DB connection.
