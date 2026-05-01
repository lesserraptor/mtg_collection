"""Image proxy endpoint: fetches card images from Scryfall and caches locally."""

from pathlib import Path

import httpx
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import FileResponse

router = APIRouter()

IMAGE_CACHE = Path.home() / ".cache" / "mtga" / "images"
IMAGE_CACHE.mkdir(parents=True, exist_ok=True)


@router.get("/images/{arena_id}")
async def proxy_image(arena_id: int, request: Request):
    """Proxy a card image from Scryfall, caching by scryfall_id UUID.

    Cache key is scryfall_id (not the URI) so cache survives Scryfall URI format changes.
    """
    db = request.app.state.db
    row = db.execute(
        "SELECT scryfall_id, image_uri_front FROM cards WHERE arena_id = ?",
        (arena_id,),
    ).fetchone()

    if not row or not row["image_uri_front"]:
        # Fallback: find another printing of the same card that has an image
        if row:
            fallback = db.execute(
                """SELECT scryfall_id, image_uri_front FROM cards
                   WHERE LOWER(name) = LOWER((SELECT name FROM cards WHERE arena_id=?))
                   AND image_uri_front IS NOT NULL LIMIT 1""",
                (arena_id,),
            ).fetchone()
            if fallback:
                row = fallback
            else:
                raise HTTPException(status_code=404, detail=f"No image for arena_id={arena_id}")
        else:
            raise HTTPException(status_code=404, detail=f"No image for arena_id={arena_id}")

    # Cache path uses stable scryfall_id UUID, not URI path components
    cache_path = IMAGE_CACHE / f"{row['scryfall_id']}.jpg"

    if not cache_path.exists():
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.get(row["image_uri_front"], follow_redirects=True)
            resp.raise_for_status()
            cache_path.write_bytes(resp.content)

    return FileResponse(cache_path, media_type="image/jpeg")


@router.delete("/images/{arena_id}/cache")
async def clear_image_cache(arena_id: int, request: Request):
    """Delete the cached image file(s) for a card, forcing a fresh fetch on next load."""
    db = request.app.state.db
    row = db.execute(
        "SELECT scryfall_id FROM cards WHERE arena_id = ?", (arena_id,)
    ).fetchone()
    if not row or not row["scryfall_id"]:
        raise HTTPException(status_code=404, detail=f"No card for arena_id={arena_id}")

    for suffix in ["", "_back"]:
        path = IMAGE_CACHE / f"{row['scryfall_id']}{suffix}.jpg"
        if path.exists():
            path.unlink()

    return {"cleared": True}


@router.get("/images/{arena_id}/back")
async def proxy_image_back(arena_id: int, request: Request):
    """Proxy the back-face image for a DFC card from Scryfall, caching separately.

    Cache key is {scryfall_id}_back.jpg to avoid collision with front-face cache.
    Returns 404 if image_uri_back is NULL (card has no back face).
    """
    db = request.app.state.db
    row = db.execute(
        "SELECT scryfall_id, image_uri_back FROM cards WHERE arena_id = ?",
        (arena_id,),
    ).fetchone()

    if not row or not row["image_uri_back"]:
        raise HTTPException(status_code=404, detail=f"No back-face image for arena_id={arena_id}")

    # Separate cache key from front face to avoid collision
    cache_path = IMAGE_CACHE / f"{row['scryfall_id']}_back.jpg"

    if not cache_path.exists():
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.get(row["image_uri_back"], follow_redirects=True)
            resp.raise_for_status()
            cache_path.write_bytes(resp.content)

    return FileResponse(cache_path, media_type="image/jpeg")
