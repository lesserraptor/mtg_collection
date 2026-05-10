# Draft Results Tracker - Implementation Plan

## Overview
Track draft results in database, capture new drafts from logs, display ROI chart on metrics page.

## 1. Database Schema (`src/db/schema.py`)

Add `draft_results` table:
```sql
CREATE TABLE draft_results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date TEXT NOT NULL,
    set_code TEXT NOT NULL,
    format TEXT NOT NULL,
    wins INTEGER NOT NULL,
    losses INTEGER NOT NULL,
    cost_gold INTEGER NOT NULL,
    winnings_gems INTEGER NOT NULL,
    trophy INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX idx_draft_results_date ON draft_results(date);
```

## 2. Import CSV Data

Create `scripts/import_draft_results.py`:
- Read `data/draft_results.csv` (96 rows, skip empty)
- Parse dates from "M/D/YYYY H:MM" → "YYYY-MM-DD"
- Map CSV columns to DB:
  - Date → date
  - Set → set_code
  - Format → format
  - w → wins
  - l → losses
  - Cost → cost_gold
  - Winnings → winnings_gems
  - Trophy ("x") → trophy (1 or 0)

## 3. Insert Latest Draft

Manual insert:
```sql
INSERT INTO draft_results (date, set_code, format, wins, losses, cost_gold, winnings_gems, trophy)
VALUES ('2026-05-09', 'SOS', 'PickTwoDraft', 3, 2, 6000, 1000, 0);
```

## 4. Log Scanner Updates (`src/draft/log_scanner.py`)

### Add reward lookup:
```python
REWARD_LOOKUP = {
    "PickTwoDraft": {0: 50, 1: 150, 2: 800, 3: 1000, 4: 1300},
    "QuickDraft": {0: 50, 1: 100, 2: 200, 3: 300, 4: 450, 5: 650, 6: 850, 7: 950},
    "PremierDraft": {0: 50, 1: 100, 2: 250, 3: 1000, 4: 1400, 5: 1600, 6: 1800, 7: 2200},
    "TradDraft": {0: 200, 1: 500, 2: 1200, 3: 1800, 4: 2200},
}
```

### Update DraftState (`src/draft/state.py`):
Add fields:
- entry_cost_gold: int = 0
- wins: int = 0

### On draft start (EventJoin):
- Parse entry_cost_gold from request ("EntryCurrencyPaid")
- Store format from mapping

### Track wins during matches:
- Parse match completion events ("matchGameRoomStateChangedEvent")
- Track winningTeamId to increment wins

### On draft complete (EventClaimPrize):
- Get wins from CurrentWins in response
- Look up winnings_gems from REWARD_LOOKUP
- Determine trophy: PickTwoDraft ≥4 wins, others ≥7 wins
- Insert record to DB via app.state.db

## 5. Metrics Chart

### New endpoint (`src/web/routes/metrics.py`):
```python
@router.get("/metrics/draft-data")
async def draft_roi_data(request: Request):
    db = request.app.state.db
    rows = db.execute("""
        SELECT date, cost_gold, winnings_gems
        FROM draft_results ORDER BY date
    """).fetchall()

    labels = [r["date"] for r in rows]
    ratios = [r["winnings_gems"] / r["cost_gold"] for r in rows]

    # Rolling averages
    avg10 = rolling_average(ratios, 10)
    avg30 = rolling_average(ratios, 30)
    avg50 = rolling_average(ratios, 50)

    return {"labels": labels, "points": ratios, "avg10": avg10, "avg30": avg30, "avg50": avg50}
```

### Update template (`src/web/templates/metrics.html`):
- Add chart div with taller height (300px): `<canvas id="chart-draft-roi">`
- Fetch `/metrics/draft-data`
- Render scatter-style chart (line type with `showLine: false` for points only)
- Show ROI points with reduced opacity (50%) and small radius (4px)
- Add horizontal dashed red line at y=0.1 (break even)
- Overlay 3 moving average lines (10, 30, 50 matches)
- Y-axis fixed at min 0

## Format Mapping
Log scanner uses these format names:
- PickTwoDraft
- QuickDraft
- PremierDraft
- TradDraft