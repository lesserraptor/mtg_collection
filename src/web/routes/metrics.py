"""Metrics route: track wallet and collection metrics over time."""

from datetime import date, timedelta
from typing import Any

from fastapi import APIRouter, Form, Query, Request, status
from fastapi.responses import RedirectResponse

from src.web.routes._set_info import _ensure_set_info, _SET_INFO

router = APIRouter()

GEMS_RECOVERED_BONUS = 3400


def _get_date_range(range_param: str) -> tuple[date, date | None]:
    """Convert range param to start/end dates."""
    today = date.today()
    end = today

    if range_param == "all":
        return (date(2000, 1, 1), None)
    elif range_param == "365":
        start = today - timedelta(days=365)
    elif range_param == "180":
        start = today - timedelta(days=180)
    elif range_param == "90":
        start = today - timedelta(days=90)
    elif range_param == "30":
        start = today - timedelta(days=30)
    else:
        start = today - timedelta(days=30)

    return (start, end)


def _forward_fill(rows: list[dict], start: date, end: date) -> list[dict]:
    """Fill missing dates with previous values."""
    if not rows:
        return []

    data_by_date = {row["date"]: row for row in rows}

    result = []
    current = start
    last_values: dict[str, Any] = {}

    while current <= end:
        date_str = current.isoformat()
        if date_str in data_by_date:
            last_values = data_by_date[date_str].copy()
            result.append(last_values)
        elif last_values:
            filled = last_values.copy()
            filled["date"] = date_str
            result.append(filled)

        current += timedelta(days=1)

    return result


@router.get("/metrics")
async def metrics_view(request: Request, range: str = "90", sort: str = "set", dir: str = "desc"):
    """Render the metrics page with chart."""
    db = request.app.state.db
    templates = request.app.state.templates

    purchases = db.execute(
        "SELECT id, purchase_date, gems_at_purchase FROM mastery_pass_purchases ORDER BY purchase_date DESC LIMIT 5"
    ).fetchall()

    # OM1 was renamed to SPM after those drafts were recorded, so normalize
    # the set code before grouping so historical drafts group under SPM.
    draft_set_stats = [
        dict(r)
        for r in db.execute("""
            WITH normalized AS (
                SELECT
                    CASE WHEN LOWER(d.set_code) = 'om1' THEN 'spm' ELSE d.set_code END AS set_code,
                    d.cost_gold, d.winnings_gems, d.trophy
                FROM draft_results d
            )
            SELECT n.set_code, COUNT(*) AS drafts,
                   AVG(CASE WHEN n.cost_gold > 0 THEN CAST(n.winnings_gems AS REAL) / n.cost_gold ELSE 0 END) AS avg_roi,
                   SUM(n.trophy) AS trophies, SUM(n.winnings_gems) AS gems_won,
                   AVG(n.winnings_gems) AS avg_gems_per_draft,
                   SUM(n.cost_gold) AS total_gold_spent,
                   SUM(n.cost_gold) * 1.0 / NULLIF(SUM(n.winnings_gems), 0) AS gold_per_gem,
                   COALESCE((SELECT MAX(c.arena_id) FROM cards c
                             WHERE LOWER(c.set_code) = LOWER(n.set_code)
                               AND c.is_rebalanced = 0
                               AND c.rarity NOT IN ('basic')
                               AND c.is_active = 1), 0) AS max_arena_id
            FROM normalized n
            GROUP BY n.set_code
        """).fetchall()
    ]

    _ensure_set_info(db)
    for row in draft_set_stats:
        info = _SET_INFO.get(row["set_code"].lower(), {})
        row["set_name"] = info.get("name") or row["set_code"].upper()
        row["icon_svg_uri"] = info.get("icon_svg_uri", "")

    sort_columns = {"set", "drafts", "avg_roi", "trophies", "gems_won",
                    "avg_gems_per_draft", "total_gold_spent", "gold_per_gem"}
    if sort not in sort_columns:
        sort = "set"
    reverse = (dir == "desc")

    def _sort_key(row):
        if sort == "set":
            return row.get("max_arena_id") or 0
        return row.get(sort, 0) or 0

    draft_set_stats.sort(key=_sort_key, reverse=reverse)

    totals_row = db.execute("""
        SELECT COUNT(*) AS drafts,
               SUM(CASE WHEN cost_gold > 0 THEN CAST(winnings_gems AS REAL) ELSE 0 END)
                   / NULLIF(SUM(CASE WHEN cost_gold > 0 THEN cost_gold ELSE 0 END), 0) AS avg_roi,
               SUM(trophy) AS trophies,
               SUM(winnings_gems) AS gems_won,
               AVG(winnings_gems) AS avg_gems_per_draft,
               SUM(cost_gold) AS total_gold_spent,
               SUM(cost_gold) * 1.0 / NULLIF(SUM(winnings_gems), 0) AS gold_per_gem
        FROM draft_results
    """).fetchone()
    draft_set_totals = dict(totals_row) if totals_row else {
        "drafts": 0,
        "avg_roi": 0.0,
        "trophies": 0,
        "gems_won": 0,
        "avg_gems_per_draft": 0.0,
        "total_gold_spent": 0,
        "gold_per_gem": 0.0,
    }
    if draft_set_totals["avg_roi"] is None:
        draft_set_totals["avg_roi"] = 0.0

    return templates.TemplateResponse(request, "metrics.html", {
        "mode": "metrics",
        "range": range,
        "sort": sort,
        "dir": dir,
        "mastery_purchases": [dict(r) for r in purchases],
        "draft_set_stats": draft_set_stats,
        "draft_set_totals": draft_set_totals,
    })


@router.get("/metrics/data")
async def metrics_data(request: Request, range: str = "90"):
    """Return metrics data as JSON for Chart.js."""
    db = request.app.state.db
    start_date, _ = _get_date_range(range)

    query = """
        SELECT date, gems, gold, mythic_wc, rare_wc, uncommon_wc,
               common_wc, draft_tokens, total_cards
        FROM wallet_snapshots
        WHERE date >= ?
        ORDER BY date ASC
    """
    rows = db.execute(query, (start_date.isoformat(),)).fetchall()

    if not rows:
        return {
            "labels": [],
            "datasets": [],
        }

    data = [dict(r) for r in rows]

    start = date.fromisoformat(data[0]["date"]) if data else date.today()
    end = date.today()
    filled = _forward_fill(data, start, end)

    labels = [row["date"] for row in filled]

    baseline_row = db.execute(
        "SELECT gems_at_purchase FROM mastery_pass_purchases ORDER BY purchase_date DESC LIMIT 1"
    ).fetchone()
    baseline = baseline_row["gems_at_purchase"] if baseline_row else None

    datasets = [
        {"label": "Gems", "data": [row["gems"] for row in filled], "borderColor": "#a855f7", "backgroundColor": "rgba(168,85,247,0.1)"},
        {"label": "Gold", "data": [row["gold"] for row in filled], "borderColor": "#eab308", "backgroundColor": "rgba(234,179,8,0.1)"},
        {"label": "Mythic WC", "data": [row["mythic_wc"] for row in filled], "borderColor": "#f97316", "backgroundColor": "rgba(249,115,22,0.1)"},
        {"label": "Rare WC", "data": [row["rare_wc"] for row in filled], "borderColor": "#3b82f6", "backgroundColor": "rgba(59,130,246,0.1)"},
        {"label": "Uncommon WC", "data": [row["uncommon_wc"] for row in filled], "borderColor": "#22c55e", "backgroundColor": "rgba(34,197,94,0.1)"},
        {"label": "Common WC", "data": [row["common_wc"] for row in filled], "borderColor": "#6b7280", "backgroundColor": "rgba(107,114,128,0.1)"},
        {"label": "Draft Tokens", "data": [row["draft_tokens"] for row in filled], "borderColor": "#ec4899", "backgroundColor": "rgba(236,72,153,0.1)"},
        {"label": "Total Cards", "data": [row["total_cards"] for row in filled], "borderColor": "#14b8a6", "backgroundColor": "rgba(20,184,166,0.1)"},
    ]

    if baseline is not None:
        datasets.append(
            {"label": "Mastery Purchase Baseline", "data": [baseline] * len(filled), "borderColor": "#22c55e", "backgroundColor": "rgba(34,197,94,0)", "borderDash": [5, 5], "fill": False, "pointRadius": 3, "tension": 0, "borderWidth": 2}
        )
        datasets.append(
            {"label": "Gems Recovered", "data": [baseline + GEMS_RECOVERED_BONUS] * len(filled), "borderColor": "#f59e0b", "backgroundColor": "rgba(245,158,11,0)", "borderDash": [2, 2], "fill": False, "pointRadius": 3, "tension": 0, "borderWidth": 2}
        )

    return {
        "labels": labels,
        "datasets": datasets,
    }


@router.post("/metrics/mastery")
async def add_mastery_purchase(
    request: Request,
    purchase_date: str = Form(...),
    gems_at_purchase: int = Form(...),
):
    """Record a new mastery pass purchase."""
    db = request.app.state.db
    db.execute(
        "INSERT INTO mastery_pass_purchases (purchase_date, gems_at_purchase) VALUES (?, ?)",
        (purchase_date, gems_at_purchase),
    )
    db.commit()
    return RedirectResponse(url="/metrics", status_code=status.HTTP_302_FOUND)


@router.post("/metrics/mastery/delete")
async def delete_mastery_purchase(request: Request, id: int = Form(...)):
    """Delete a mastery pass purchase."""
    db = request.app.state.db
    db.execute("DELETE FROM mastery_pass_purchases WHERE id = ?", (id,))
    db.commit()
    return RedirectResponse(url="/metrics", status_code=status.HTTP_302_FOUND)


def _rolling_average(values: list[float], window: int) -> list[float | None]:
    """Calculate rolling average with None for insufficient data points."""
    result = []
    for i, v in enumerate(values):
        if i < window - 1:
            result.append(None)
        else:
            avg = sum(values[i - window + 1 : i + 1]) / window
            result.append(avg)
    return result


@router.get("/metrics/draft-data")
async def draft_roi_data(request: Request):
    """Return draft ROI data as JSON for Chart.js."""
    db = request.app.state.db
    rows = db.execute("""
        SELECT date, cost_gold, winnings_gems
        FROM draft_results ORDER BY date
    """).fetchall()

    if not rows:
        return {"labels": [], "points": [], "avg10": [], "avg30": [], "avg50": []}

    labels = [r["date"] for r in rows]
    ratios = [r["winnings_gems"] / r["cost_gold"] if r["cost_gold"] > 0 else 0 for r in rows]

    avg10 = _rolling_average(ratios, 10)
    avg30 = _rolling_average(ratios, 30)
    avg50 = _rolling_average(ratios, 50)

    return {"labels": labels, "points": ratios, "avg10": avg10, "avg30": avg30, "avg50": avg50}