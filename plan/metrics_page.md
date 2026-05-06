# Metrics Page Plan

## Overview
Add a new "Resources" tab (originally "Metrics") that tracks wallet and collection resources over time, displayed in individual graphs.

## Metrics Tracked
- Gems
- Gold
- Wildcards by rarity (Mythic, Rare, Uncommon, Common) - grouped on one chart
- Draft Tokens
- Total Cards in collection (matching collection page count: distinct card names with quantity > 0)

## Data Source
- Wallet data extracted from MTGA Player.log's `InventoryInfo` in StartHook events
- Card count from the `collection` table

### Player.log InventoryInfo Structure
```
InventoryInfo:
  Gems: <int>
  Gold: <int>
  WildCardMythics: <int>
  WildCardRares: <int>
  WildCardUnCommons: <int>
  WildCardCommons: <int>
  CustomTokens:
    DraftToken: <int>
```

## Data Capture Strategy
1. Capture wallet snapshots when collection is updated (via file watcher or manual reload)
2. Store one record per day - latest value for that day overwrites previous
3. Forward-fill missing dates at display time (show previous day's value if no data for a given day)

## Time Frames
- 30 days
- 90 days (default)
- 180 days
- 1 year
- All time

## UI Layout
Each metric has its own graph:
- Gems - separate chart
- Gold - separate chart
- Wildcards (all rarities) - combined chart
- Draft Tokens - separate chart
- Total Cards - separate chart

## Files Modified
- `src/db/schema.py` - Added `wallet_snapshots` table
- `src/db/log_parser.py` - Added `parse_log_wallet()` function
- `src/collection.py` - Added `_capture_wallet_snapshot()` called after collection upsert
- `src/web/routes/metrics.py` - New route with page and JSON endpoint
- `src/web/app.py` - Registered metrics router
- `src/web/templates/base.html` - Added Resources tab to nav
- `src/web/templates/metrics.html` - New template with Chart.js
- `src/web/static/app.css` - Added metrics page styling

## Bug Fixes
- **Vertical scrollbars** - Changed `.metrics-page` to `flex: 1; overflow-y: auto` so the page itself scrolls within the existing `.main` flex container
- **Decimals in Total Cards** - Added `ticks: { callback: (value) => Number.isInteger(value) ? value : null }` to all chart scale configs to show only integers on Y-axis