# 17lands card metrics from public game-data CSV

Reference for computing 17lands card ratings locally from the public game-data CSV, for later tooling in this repo.

Last verified 2026-10-30.

## 1. What the file is

`data/game_data_public.FRA.PremierDraft.csv` — 879 MB, 297,964 rows. 17lands public game data for the FRA (Reality Fracture) Premier Draft format.

**One row per GAME**, not per draft. A single draft contributes one row per game played in it.

Column layout:

| Group | Columns |
|---|---|
| Metadata | `expansion`, `event_type`, `draft_id`, `draft_time`, `game_time`, `build_index`, `match_number`, `game_number`, `rank`, `opp_rank`, `main_colors`, `splash_colors`, `on_play`, `num_mulligans`, `opp_num_mulligans`, `opp_colors`, `num_turns`, `won` |
| Per card (repeated for every card in the set, alphabetically) | `opening_hand_<Card>`, `drawn_<Card>`, `tutored_<Card>`, `deck_<Card>`, `sideboard_<Card>` |

The per-card block means 5 columns × hundreds of cards — over 2000 columns total. Per-card meaning:

- `opening_hand_<Card>` — flag: card started in the opening hand
- `drawn_<Card>` — flag: card was drawn at some point after the opening hand
- `tutored_<Card>` — flag: card was put into hand/tutor-reachable without being drawn
- `deck_<Card>` — copy count of the card in the build
- `sideboard_<Card>` — copy count in the sideboard

## 2. Column semantics (verified)

- Per-card flags are **99.99% binary**. Observed maxes over the full file: `opening_hand_` max = 2 (only 3 games ≥ 2), `drawn_` max = 3 (20 games ≥ 2).
- `won` is `True`/`False` as text.
- `rank` is a prefix string only: `bronze`, `silver`, `gold`, `platinum`, `diamond`, `mythic`, or blank. No tier numbers (e.g. no `Diamond-2`).
- Same `draft_id` = same draft. The first rows of the file (rows 2–4) are 3 games of one WG draft.
- `build_index` identifies the deck build version within a draft (a player can rebuild between games).
- `deck_<Card>` values sum to 40 and `sideboard_<Card>` values sum to 15 per game (per game row).

## 3. Data window

`game_time` / `draft_time` span **2026-09-29 → 2026-10-16**, roughly 17 days.

Website numbers dated later than that (checked 2026-10-30) include games absent from this file. Any comparison of local numbers against the live site must account for the staleness gap.

## 4. Mapping ratings-API fields to computations

All keyed on "Ajani Unrelenting"; every number below was measured from the CSV (and its API counterpart). See `data/ajani_unrelenting_local_vs_api.md` for the full comparison report.

### GIHWR — "games in hand" win rate

```
gih_game   = (opening_hand == 1) OR (drawn == 1)
gihwr      = mean(won) over gih_game rows
```

| Segment | n | WR |
|---|---|---|
| All ranks (Ajani) | 4,322 | 73.99% |
| Website "All players" GIHWR | — | 73.6% |

Matches within the data-window gap.

### Instance weighting

Official definition: each copy/instance counts, so an opening-hand copy plus 2 later draws of a second copy = 3× weight in one game. Proxy computable from the flags:

```
weight = opening_hand + drawn        # 0–3 typically, flag max jackpots add more
```

- Proxy result: 74.11% (vs 73.99% for the binary definition) — difference ≤ 0.2 pp.
- Why so small: multi-copy in-hand rows are only 1.9% of games (79 games with 2 copies, 5 with 3).
- The exact official weighting is **not fully reconstructable** from the public flags: draw multiplicities beyond the flag value are invisible in most rows.

### Rank filtering

| Segment | n | WR |
|---|---|---|
| diamond ∪ mythic, in-hand | 486 | 75.10% |
| diamond only, in-hand | 387 | 75.97% |
| mythic only, in-hand | 99 | 71.72% (small cell) |

### "Top players" GIHWR — not reproducible

Website shows 78.5%; a plain rank filter caps at 75–76% (`diamond ∪ mythic`, above). Zero values exist in the 78.5 ± 0.7 pp band for plain rank groupings. Closest defensible local candidate:

```
opening_hand == 1  AND  rank ∈ {diamond, mythic}  AND  on_play == True
→ 78.41%, n = 88
```

Larger-n candidates all feature `on_play == True` heavily (396 of 414 games). Conclusion: the top-player lift likely involves on-play selection and/or games newer than this CSV's window.

### Deck-present win rate

```
deck_present = deck_<Card> >= 1
```

| Segment | n | WR |
|---|---|---|
| All ranks | 8,975 | 63.38% |
| diamond ∪ mythic | 984 | 64.33% |

### drawn_improvement_win_rate

API field is null for this card, but it is derivable locally:

```
drawn_improvement = drawn WR − never-drawn WR
drawn        = drawn == 1
never-drawn  = drawn == 0 AND opening_hand == 0
→ 73.80% (n = 2,771) − 55.15% = +18.65 pp
```

### API-entry arithmetic checks

These verified against the ratings API entry for Ajani:

- `ever_drawn_game_count` = `opening_hand_game_count` + `drawn_game_count` — 85 + 158 = 243, exact.
- `game_count` ≈ in-hand games + tutored-only games. Tutoring counts as in-hand on the API side; the CSV has 126 tutored-only rows (WR 73.02%).

### Not computable from the game-data CSV

`seen_count`, `avg_seen`, `pick_count`, `avg_pick` are draft-side metrics. They come from a separate data source (draft logs / ratings API), not the game-data file.

## 5. Open oddity — ratings API anomaly

The `/card_ratings/data` API entry for FRA fetched on 2026-10-30 returned Ajani with `game_count = 481` / `pool_count = 498` — **smaller** than this (older-window) CSV's 4,322 in-hand games — and all win rates null.

Across all 285 comparable FRA entries, the per-card CSV-to-API ratios range 4.9×–65× (median ≈ 19.8×), which rules out uniform subsampling.

Unresolved. Next probe: one call each with no `format` param, `format=All`, and `colors` variants, or a fresh fetch to compare against `data/17lands_card_ratings_reality_fracture.json`.

## 6. How to process it

**Never load the whole file** — 2000+ columns will exhaust memory. Stream with `csv.reader`, accumulating only the target card's columns plus the shared metadata columns. One card over all 297,964 rows runs in ~10 s.

Working scripts live in `/tmp/opencode` (**may be wiped on reboot** — copy anything worth keeping into the repo):

| Script | What it does |
|---|---|
| `compute_card_metrics.py` | Basic metrics + API comparison |
| `gihwr_hunt.py` | Partition search for matching the website GIHWR |
| `gihwr_hunt2.py` | Rank/week/on-play grid, 243 combinations |
| `gih_weighted.py` | Instance-weight probe |
| `rank_wr_quick.py` | Rank-union win rates |

Reports at the same locations, plus `data/ajani_unrelenting_local_vs_api.md` (committed-adjacent only — `data/` is gitignored, see AGENTS.md).

## 7. Related saved artifacts

| Path | What |
|---|---|
| `data/17lands_card_ratings_reality_fracture.json` | Ratings API response, FRA PremierDraft, 290 cards, fetched 2026-10-30 |
| `data/17lands_card_ratings_sample.json` | Ratings API response, DFT, 281 cards |
| `src/draft/seventeen_lands.py:12-49` | The app's own ratings call site |

Ratings API call shape:

```
GET https://www.17lands.com/card_ratings/data?expansion=<SET>&format=<FMT>
```

No auth or special headers needed; the app uses `httpx` with a 15 s timeout. Response is a JSON list of per-card metric objects.

## 8. Build ideas for later

- Streaming per-card metric calculator (CSV → a ratings-shaped table locally)
- Rank-sharded ratings (own "top players" definition, since the website's isn't reproducible)
- `drawn_improvement_win_rate` charts for cards where the API field is null
- Weekly WR trend per card (`draft_time`/`game_time` bucketing)
- Draft-color-shard filters using `main_colors` / `splash_colors`
- Fresh-CSV ingest + staleness detection against the live API (window comparison)
