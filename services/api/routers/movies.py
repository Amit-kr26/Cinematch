# services/api/routers/movies.py
import json
import logging

import tmdb
from deps import get_http, get_pg, get_redis
from fastapi import APIRouter, Depends, HTTPException, Query

router = APIRouter()
logger = logging.getLogger(__name__)

# MovieLens 1M genre vocabulary (exact strings as stored in movies.dat)
GENRES = [
    "Action", "Adventure", "Animation", "Children's", "Comedy", "Crime",
    "Documentary", "Drama", "Fantasy", "Film-Noir", "Horror", "Musical",
    "Mystery", "Romance", "Sci-Fi", "Thriller", "War", "Western",
]


@router.get("/genres")
async def genres():
    return {"genres": GENRES}


@router.get("/movies")
async def list_movies(
    search: str | None = None,
    genre: str | None = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=60),
    redis=Depends(get_redis),
    pg=Depends(get_pg),
    http=Depends(get_http),
):
    """Browse/search the full catalogue from PostgreSQL, ordered by popularity."""
    if pg is None:
        return {"movies": [], "page": page, "page_size": page_size, "total": 0, "has_more": False}

    conds: list[str] = []
    args: list = []
    if genre and genre != "All":
        args.append(f"%{genre}%")
        conds.append(f"genres ILIKE ${len(args)}")
    if search:
        args.append(f"%{search.strip()}%")
        conds.append(f"title ILIKE ${len(args)}")
    where = ("WHERE " + " AND ".join(conds)) if conds else ""

    async with pg.acquire() as conn:
        total = await conn.fetchval(f"SELECT count(*) FROM movies {where}", *args)
        rows = await conn.fetch(
            f"SELECT movie_id, title, genres, year FROM movies {where} "
            f"ORDER BY popularity DESC NULLS LAST, movie_id "
            f"LIMIT ${len(args) + 1} OFFSET ${len(args) + 2}",
            *args, page_size, (page - 1) * page_size,
        )

    items = [dict(r) for r in rows]
    await tmdb.enrich(redis, http, items)
    total = total or 0
    return {
        "movies": items,
        "page": page,
        "page_size": page_size,
        "total": total,
        "has_more": page * page_size < total,
    }


@router.get("/movies/{movie_id}/similar")
async def movie_similar(movie_id: int, redis=Depends(get_redis),
                        pg=Depends(get_pg), http=Depends(get_http)):
    raw = await redis.get(f"item_sim:{movie_id}")
    if not raw:
        return {"movie_id": movie_id, "similar": []}

    neighbors = json.loads(raw)[:6]
    ids = [n["movie_id"] for n in neighbors]

    rows: dict[int, dict] = {}
    if pg is not None:
        async with pg.acquire() as conn:
            fetched = await conn.fetch(
                "SELECT movie_id, title, genres, year FROM movies WHERE movie_id = ANY($1)", ids
            )
            rows = {r["movie_id"]: dict(r) for r in fetched}

    items = [rows[i] for i in ids if i in rows]
    await tmdb.enrich(redis, http, items)
    by_id = {it["movie_id"]: it for it in items}

    similar = []
    for n in neighbors:
        it = by_id.get(n["movie_id"])
        if it:
            similar.append({**it, "similarity": round(float(n["similarity"]), 3)})
    return {"movie_id": movie_id, "similar": similar}


@router.get("/movies/{movie_id}")
async def movie_detail(movie_id: int, redis=Depends(get_redis),
                       pg=Depends(get_pg), http=Depends(get_http)):
    if pg is None:
        raise HTTPException(status_code=503, detail="Database unavailable")
    async with pg.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT movie_id, title, genres, year FROM movies WHERE movie_id = $1", movie_id
        )
    if not row:
        raise HTTPException(status_code=404, detail="Movie not found")

    item = dict(row)
    await tmdb.enrich(redis, http, [item])
    sim = await movie_similar(movie_id, redis=redis, pg=pg, http=http)
    return {"movie": item, "similar": sim["similar"]}
