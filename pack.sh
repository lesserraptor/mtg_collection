#!/usr/bin/env bash
# pack.sh — bundle the app for transfer to another machine
# Usage: ./pack.sh
# Output: mtg-YYYYMMDD.zip in the current directory

set -euo pipefail

OUTFILE="mtg-$(date +%Y%m%d).zip"

git archive --format=zip --prefix=mtg/ HEAD -o "$OUTFILE"

echo "Created: $OUTFILE"
echo "Size:    $(du -sh "$OUTFILE" | cut -f1)"
echo ""
echo "On the new machine:"
echo "  unzip $OUTFILE"
echo "  cd mtg"
echo "  python3 -m venv .venv"
echo "  source .venv/bin/activate"
echo "  pip install -r requirements.txt"
echo "  python -m uvicorn src.web.app:app --host 0.0.0.0 --port 8000"
