#!/usr/bin/env python3
"""Import draft results from CSV into the database."""

import csv
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.db.schema import open_db, create_schema, create_indexes


def parse_date(date_str: str) -> str:
    """Parse 'M/D/YYYY H:MM' → 'YYYY-MM-DD'."""
    dt = datetime.strptime(date_str.strip(), "%m/%d/%Y %H:%M")
    return dt.strftime("%Y-%m-%d")


def main():
    db_path = Path(__file__).parent.parent / "data" / "mtga_collection.db"
    csv_path = Path(__file__).parent.parent / "data" / "draft_results.csv"

    if not csv_path.exists():
        print(f"Error: {csv_path} not found")
        sys.exit(1)

    conn = open_db(db_path)
    create_schema(conn)
    create_indexes(conn)

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        if header[0].startswith('\ufeff'):
            header[0] = header[0].replace('\ufeff', '')
        rows = [r for r in reader if r and r[0].strip()]

    # Header: Date,Set,Trophy,Colors,w,w,l,Format,Start Rank,End Rank,Cost,Winnings
    # Col indices:    0    1    2      3     4  5  6    7       8          9      10      11
    date_idx = header.index("Date")
    set_idx = header.index("Set")
    trophy_idx = header.index("Trophy")
    wins_idx = 5  # second "w" column
    loss_idx = header.index("l")
    format_idx = header.index("Format")
    cost_idx = header.index("Cost")
    winnings_idx = header.index("Winnings")

    print(f"Importing {len(rows)} rows from {csv_path}")

    inserted = 0
    for row in rows:
        date = parse_date(row[date_idx])
        set_code = row[set_idx].strip()
        fmt = row[format_idx].strip()
        wins = int(row[wins_idx])
        losses = int(row[loss_idx])
        cost_gold = int(row[cost_idx])
        winnings_gems = int(row[winnings_idx]) if row[winnings_idx].strip() else 0
        trophy = 1 if row[trophy_idx].strip() == "x" else 0

        conn.execute(
            """
            INSERT INTO draft_results (date, set_code, format, wins, losses, cost_gold, winnings_gems, trophy)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (date, set_code, fmt, wins, losses, cost_gold, winnings_gems, trophy),
        )
        inserted += 1

    conn.commit()
    print(f"Inserted {inserted} draft results")

    count = conn.execute("SELECT COUNT(*) as cnt FROM draft_results").fetchone()[0]
    print(f"Total rows in draft_results: {count}")


if __name__ == "__main__":
    main()