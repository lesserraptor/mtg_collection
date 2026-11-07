"""wallet.py — Wallet/currency snapshot from Player.log.

Extracts gold, gems, wildcards, and draft tokens from MTGA's Player.log
and persists them to the wallet_snapshots table, independent of collection upsert.
"""

import sqlite3
from datetime import date
from pathlib import Path

from src.db.log_parser import find_player_log, parse_log_wallet

logger = __import__("logging").getLogger(__name__)


def capture_wallet_snapshot(conn: sqlite3.Connection) -> None:
    """Capture current wallet state to wallet_snapshots table.

    Reads wallet data from Player.log via parse_log_wallet().
    Uses INSERT OR REPLACE to keep latest value per day.
    """
    log_path = find_player_log(conn)
    if log_path is None:
        logger.warning("wallet: Player.log not found, skipping snapshot")
        return
    capture_wallet_snapshot_from_log(log_path, conn)


def capture_wallet_snapshot_from_log(log_path: Path, conn: sqlite3.Connection) -> bool:
    """Capture wallet snapshot using an explicit Player.log path.

    Returns True when the stored row for today changed, so callers can log
    only meaningful wallet updates. Useful for the file watcher which already
    has the path.
    """
    today = date.today().isoformat()

    wallet = parse_log_wallet(log_path)
    if wallet is None:
        logger.debug("wallet: parse_log_wallet returned None for %s", log_path)
        return False

    total_cards = conn.execute("""
        SELECT COUNT(DISTINCT LOWER(c.name))
        FROM cards c
        JOIN collection col ON c.arena_id = col.arena_id
        WHERE col.quantity > 0
    """).fetchone()[0] or 0

    values = (
        wallet.get("gems", 0),
        wallet.get("gold", 0),
        wallet.get("mythic_wc", 0),
        wallet.get("rare_wc", 0),
        wallet.get("uncommon_wc", 0),
        wallet.get("common_wc", 0),
        wallet.get("draft_tokens", 0),
        total_cards,
    )

    previous = conn.execute("""
        SELECT gems, gold, mythic_wc, rare_wc, uncommon_wc, common_wc, draft_tokens, total_cards
        FROM wallet_snapshots
        WHERE date = ?
    """, (today,)).fetchone()

    conn.execute("""
        INSERT OR REPLACE INTO wallet_snapshots
        (date, gems, gold, mythic_wc, rare_wc, uncommon_wc, common_wc, draft_tokens, total_cards)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (today, *values))
    conn.commit()

    if previous is not None and tuple(previous) == values:
        logger.debug(
            "wallet: snapshot unchanged for %s (gems=%s, gold=%s)", today, values[0], values[1]
        )
        return False

    logger.info(
        "wallet: snapshot changed for %s (gems=%s, gold=%s)", today, values[0], values[1]
    )
    return True
