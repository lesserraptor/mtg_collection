# Moving to a New Machine

## Packaging (on this machine)

```bash
./pack.sh
```

Creates `mtg-YYYYMMDD.zip` in the project directory. Copy this file to the new machine however is convenient (USB, local network, etc.).

---

## Setup (on the Linux machine)

### 1. Unzip

```bash
unzip mtg-20260403.zip
cd mtg
```

### 2. Create virtual environment and install dependencies

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 3. Start the app

```bash
source .venv/bin/activate
python -m uvicorn src.web.app:app --host 0.0.0.0 --port 8000
```

Open a browser to `http://localhost:8000`.

### 4. Load data (first run only)

In the app, go to **Settings** and run these in order:

1. **Refresh Scryfall** — downloads card data (~100MB, takes a minute)
2. **Refresh Card DB** — reads the MTGA card database from your Steam/Proton install
3. **Refresh Collection** — imports your owned cards from the Arena log
4. **Import Decks from Log** — imports your Arena decks

The app auto-detects the Steam/Proton paths for the card DB, collection file, and Player.log. If auto-detection fails, you can set the paths manually in Settings.

---

## Starting the app after the first run

```bash
cd mtg
source .venv/bin/activate
python -m uvicorn src.web.app:app --host 0.0.0.0 --port 8000
```
