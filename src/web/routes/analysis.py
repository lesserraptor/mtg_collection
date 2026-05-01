"""Analysis mode routes: missing cards and missing decks ranked views."""

from fastapi import APIRouter, Query, Request

from src.db.analysis import get_missing_cards_ranked, get_missing_cards_decks, get_decks_ranked_by_missing

router = APIRouter()

_ALL_TYPES = {"Creature", "Instant", "Sorcery", "Enchantment", "Artifact", "Planeswalker", "Land", "Battle"}


@router.get("/analysis")
async def analysis_view(
    request: Request,
    view: str = "cards",
    type_line: list[str] = Query(default=[]),
    types_submitted: bool = False,
):
    db = request.app.state.db
    templates = request.app.state.templates

    # Scope (is_potential/is_saved, complete-deck exclusion) enforced in analysis.py query functions.
    missing_cards = get_missing_cards_ranked(db)
    missing_cards_decks = get_missing_cards_decks(db)
    missing_decks = get_decks_ranked_by_missing(db)

    selected_types = [t for t in type_line if t in _ALL_TYPES]
    if types_submitted:
        if not selected_types:
            missing_cards = []
        elif set(selected_types) != _ALL_TYPES:
            missing_cards = [
                row for row in missing_cards
                if any(t in (row["type_line"] or "") for t in selected_types)
            ]

    context = {
        "mode": "analysis",
        "view": view,  # "cards" | "decks"
        "missing_cards": missing_cards,
        "missing_cards_decks": missing_cards_decks,
        "missing_decks": missing_decks,
        "selected_types": selected_types,
        "types_submitted": types_submitted,
    }
    return templates.TemplateResponse(request, "analysis.html", context)
