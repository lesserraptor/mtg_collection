# Metrics Page Plan

## Overview
Add a new "Resources" tab (originally "Metrics") that tracks wallet and collection resources over time, displayed in individual graphs.

## Metrics Tracked
- Gems (with mastery pass baseline lines)
- Gold
- Wildcards by rarity (Mythic, Rare, Uncommon, Common) - grouped on one chart
- Draft Tokens
- Total Cards in collection (matching collection page count: distinct card names with quantity > 0)

## Mastery Pass Tracking

### New Table
```sql
CREATE TABLE IF NOT EXISTS mastery_pass_purchases (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    purchase_date TEXT NOT NULL,
    gems_at_purchase INTEGER NOT NULL
);
```

### How It Works
1. User records a mastery pass purchase via the sidebar form on the metrics page
2. Form captures: `purchase_date` (date of purchase) and `gems_at_purchase` (gem balance immediately after purchase)
3. The Gems chart displays horizontal reference lines based on the most recent purchase:
   - **Mastery Purchase Baseline** (green dashed): gem balance right after buying the pass
   - **Gems Recovered** (amber dotted): baseline + 3400 (gems earned back to cover the pass cost)
4. Baseline is pulled from the most recent purchase (ORDER BY purchase_date DESC LIMIT 1)
5. Y-axis dynamically scales to show baseline - 500 as minimum
6. Each recorded purchase can be deleted via ✕ button in the recent purchases list

### Future: Auto-Detection
- Currently manual entry only
- May be able to detect from Player.log by looking for large gem spend + timestamp
- Will test when user next purchases a mastery pass

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
- Gems - with mastery pass baseline lines (green dashed + amber dotted)
- Gold - separate chart
- Wildcards (all rarities) - combined chart
- Draft Tokens - separate chart
- Total Cards - separate chart

### Sidebar (Metrics Page)
- Form to record new mastery pass purchases (date picker + gem balance input)
- List of recent purchases with dates and gem balances

## Files Modified
- `src/db/schema.py` - Added `wallet_snapshots` table
- `src/db/log_parser.py` - Added `parse_log_wallet()` function
- `src/collection.py` - Added `_capture_wallet_snapshot()` called after collection upsert
- `src/web/routes/metrics.py` - New route with page and JSON endpoint
- `src/web/app.py` - Registered metrics router
- `src/web/templates/base.html` - Added Resources tab to nav
- `src/web/templates/metrics.html` - New template with Chart.js
- `src/web/static/app.css` - Added metrics page styling
- **`src/web/routes/metrics.py`** - Added mastery pass baseline lines to data endpoint
- **`src/web/templates/metrics.html`** - Added sidebar form, mastery baseline lines rendered on Gems chart

## Bug Fixes
- **Vertical scrollbars** - Changed `.metrics-page` to `flex: 1; overflow-y: auto` so the page itself scrolls within the existing `.main` flex container
- **Decimals in Total Cards** - Added `ticks: { callback: (value) => Number.isInteger(value) ? value : null }` to all chart scale configs to show only integers on Y-axis