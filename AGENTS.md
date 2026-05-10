# AGENTS.md — MTGA Collection Browser

## Quick start

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python -m uvicorn src.web.app:app --host 0.0.0.0 --port 8000
```

Or use `./run.sh` (assumes `.venv` exists).

## Architecture

FastAPI web app (`src/web/app.py`) with SQLite backend (`data/mtga_collection.db`). HTMX + Jinja2 templates for the frontend. No separate build step.

### Key directories

| Path | Purpose |
|---|---|
| `src/web/app.py` | FastAPI entrypoint, mounts all route routers |
| `src/web/routes/` | Page routes: cards, decks, analysis, settings, draft, sets, changes, images |
| `src/db/schema.py` | SQLite schema (tables: cards, collection, decks, deck_lines, meta, errata, deck_versions, collection_snapshots) |
| `src/db/ingest_cli.py` | Full data pipeline CLI (CardDB → Scryfall → Collection → Errata) |
| `src/db/ingest.py` | Scryfall bulk download + enrichment |
| `src/db/mtga_card_db.py` | Native MTGA CardDatabase (.mtga SQLite) parser |
| `src/collection.py` | Collection JSON parsing |
| `src/draft/` | Draft log scanner + 17Lands integration |
| `src/watcher.py` | File watcher for live collection updates |
| `src/config.py` | `PROJECT_ROOT` and `DATA_DIR` (`data/`) |

### Data flow

1. **Native CardDB** — reads `Raw_CardDatabase_*.mtga` from Steam/Proton install
2. **Scryfall enrichment** — downloads ~2GB bulk JSON, enriches with images/metadata
3. **Collection upsert** — reads collection JSON (auto-detected from Proton path)
4. **Errata** — applies manual corrections from `src/db/errata.py`

## Commands

| Task | Command |
|---|---|
| Run dev server | `./run.sh` or `python -m uvicorn src.web.app:app --host 0.0.0.0 --port 8000` |
| Full data ingest | `python src/db/ingest_cli.py` |
| Ingest without MTGA installed | `python src/db/ingest_cli.py --skip-mtga-card-db` |
| Use cached Scryfall data | `python src/db/ingest_cli.py --skip-download` |
| Skip collection load | `python src/db/ingest_cli.py --skip-collection` |
| Bundle for transfer | `./pack.sh` |

## Important notes

- **`data/` is gitignored** — contains the SQLite DB and Scryfall cache. Never commit anything from `data/`.
- **Python 3.13+** — code uses modern syntax (`Path | None`, walrus operator, etc.)
- **No test suite** — the repo has no tests or linting config
- **Draft scanner** is NOT started automatically — use the "Start Listening" button on `/draft` in the web UI
- The DB uses WAL mode with mmap for performance; connection is shared across routes with `check_same_thread=False`

## Safety rules

1. **Never delete or overwrite project data files** — Never run `rm`, `git reset --hard`, or overwrite files in `data/` without explicit user consent.
2. **Use `/tmp` for testing** — When testing DB operations, create temp DBs in `/tmp/opencode/` instead of `data/`.
3. **Confirm destructive commands** — Before any command that modifies or deletes existing project files, ask "Are you sure?" and wait for confirmation.
4. **Ask before destructive operations** — If a task requires deleting files, resetting the DB, or making irreversible changes, explicitly ask the user first.
