# MTGA Collection Browser

A local web interface for browsing and analyzing your Magic: The Gathering Arena (MTGA) collection. Built with FastAPI and HTMX, featuring deck management, collection filtering, card analysis, and draft tracking.

## Features

- **Collection Browser** — Filter your collection by color, mana cost, type, set, rarity, and more. Browse cards in a grid view with card images from Scryfall.
- **Deck Management** — Import decks from Arena's log, export in Arena-importable text format. Track which cards from a deck you own and which are missing.
- **Analysis Mode** — Find cards to craft based on how many of your potential decks need them, discover decks closest to completion, and more.
- **Draft Tracking** — Watch your draft logs and pull game data from 17Lands to see win rates, color performance, and pick statistics.
- **Live Updates** — Watches your collection file and auto-refreshes when Arena updates your collection.
- **Set Browser** — Browse cards by set, see release dates and total card counts.

### What this does that Arena does not
- The collection browser is less clunky to navigate and can show more cards at a time.
- You can copy decks off the internet and add them here to see what cards you are missing (and you can use the analysis tab to find out which missing cards are viable in more than 1 deck)
- You can see the changes to your collection over time.
- You can track your resource usage (gold, gems, wildcards, draft tokens, collection size)
- You can see the 17lands rankings of cards during a draft. There are better overlay programs out there, but this works even if overlays don't. You can also look at the draft cards in the context of your collection (ie, if a card is on a wishlist for a deck, you'll see that)

## Data Source

**This app does not extract your collection directly from MTGA.** It cannot read your account's collection from Arena's servers. You must supply your collection data manually.

However, the app can read card data from MTGA's native card database (auto-detected from your Steam/Proton install), download card images and metadata from Scryfall, and scan your Arena logs for deck and draft information.

The first time you run the app, go to **Settings** and run the refresh steps in order.

## Quick Start

Requires Python 3.13+.

```bash
# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Start the server
python -m uvicorn src.web.app:app --host 0.0.0.0 --port 8000
```

Open `http://localhost:8000` in your browser.

### Import Your Data

Before the app is useful, you need to import your data. Two options:

**Option 1: Command Line**

```bash
# Full import (requires MTGA installed)
python src/db/ingest_cli.py

# Skip MTGA CardDB (if MTGA not installed)
python src/db/ingest_cli.py --skip-mtga-card-db

# Use cached Scryfall data (faster on subsequent runs)
python src/db/ingest_cli.py --skip-download

# Skip collection load entirely (only refresh card data)
python src/db/ingest_cli.py --skip-collection
```

**Option 2: In-App**

Start the server, go to **Settings** (`http://localhost:8000/settings`), and run the refresh steps in order.

## Project Structure

| Path | Purpose |
|------|---------|
| `src/web/` | FastAPI app and routes |
| `src/db/` | SQLite schema and data ingestion |
| `src/draft/` | Draft log scanning and 17Lands integration |
| `src/collection.py` | Collection JSON parsing |
| `data/` | SQLite database and Scryfall cache (gitignored) |

## Tech Stack

- FastAPI + Uvicorn
- HTMX + Jinja2 templates (no build step)
- SQLite with WAL mode
- Scryfall for card images and metadata

## License

MIT
