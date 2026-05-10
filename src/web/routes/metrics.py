"""Metrics route: track wallet and collection metrics over time."""

from datetime import date, timedelta
from typing import Any

from fastapi import APIRouter, Form, Query, Request, status
from fastapi.responses import RedirectResponse

router = APIRouter()


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
async def metrics_view(request: Request, range: str = "90"):
    """Render the metrics page with chart."""
    db = request.app.state.db
    templates = request.app.state.templates

    purchases = db.execute(
        "SELECT id, purchase_date, gems_at_purchase FROM mastery_pass_purchases ORDER BY purchase_date DESC LIMIT 5"
    ).fetchall()

    return templates.TemplateResponse(request, "metrics.html", {
        "mode": "metrics",
        "range": range,
        "mastery_purchases": [dict(r) for r in purchases],
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
            {"label": "Gems Recovered", "data": [baseline + 3400] * len(filled), "borderColor": "#f59e0b", "backgroundColor": "rgba(245,158,11,0)", "borderDash": [2, 2], "fill": False, "pointRadius": 3, "tension": 0, "borderWidth": 2}
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