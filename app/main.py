"""CineMatch Lite — single-container demo API for HuggingFace Spaces.

Same HTTP contract as the full stack, served from one SQLite file:
  GET  /recommend/{user_id}   4-layer fallback (user_recs -> ALS -> popular)
  POST /simulate              records an event and refreshes that user's recs
  GET  /movies                catalogue browse/search (paged)
  GET  /movies/{id}           detail (similar titles not available in lite)
  GET  /genres | /stats | /events/recent | /health | /metrics
  WS   /ws/recommend/{uid}    held open so the UI shows a live connection

No Kafka, no Spark streaming, no Redis, no Postgres — the "lite" sibling of
the full deployment on the main branch.
"""
import json
import os
import time
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

from fastapi import (
    FastAPI,
    HTTPException,
    Query,
    Request,
    WebSocket,
    WebSocketDisconnect,
)
from fastapi.responses import Response
from fastapi.staticfiles import StaticFiles
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    Counter,
    Histogram,
    generate_latest,
)
from pydantic import BaseModel

from app import bootstrap_lite

DB_PATH    = os.getenv("DB_PATH", "/data/app.db")
STATIC_DIR = Path(__file__).resolve().parent / "static"
PORT       = int(os.getenv("PORT", "7860"))

GENRES = [
    "Action", "Adventure", "Animation", "Children's", "Comedy", "Crime",
    "Documentary", "Drama", "Fantasy", "Film-Noir", "Horror", "Musical",
    "Mystery", "Romance", "Sci-Fi", "Thriller", "War", "Western",
]
VALID_EVENT_TYPES = {"click", "view", "like", "rating"}

REQUEST_LATENCY = Histogram("api_latency_ms", "API request latency in ms",
                            ["endpoint"],
                            buckets=[1, 5, 10, 25, 50, 100, 250, 500])
CACHE_HITS = Counter("cache_hit_total", "Recommendation cache hits", ["result"])

# In-process state standing in for Redis + Postgres
_movies: dict[int, dict] = {}          # movie_id -> row incl. TMDB fields
_recent_events: deque[dict] = deque(maxlen=50)
_stats = {"requests": 0, "cache_hits": 0, "latency_ms": 0.0, "events": 0}


def get_variant(user_id: int, traffic_split: int = 50) -> str:
    return "treatment" if (user_id * 2654435761) % 100 < traffic_split else "control"


def _get_db():
    import sqlite3
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _enrich(rec: dict) -> dict:
    """Attach catalogue metadata (incl. cached TMDB art) to a candidate."""
    meta = _movies.get(rec.get("movie_id"))
    out = {**rec}
    if meta:
        for key in ("year", "poster", "backdrop", "overview", "tmdb_rating"):
            if meta.get(key) is not None:
                out.setdefault(key, meta[key])
    return out


def _load_user_recs(conn, user_id: int) -> list[dict]:
    row = conn.execute(
        "SELECT recs FROM user_recs WHERE user_id = ?", (user_id,)).fetchone()
    if not row:
        return []
    return [_enrich(r) for r in json.loads(row["recs"])]


@asynccontextmanager
async def _lifespan(app: FastAPI):
    t0 = time.monotonic()
    bootstrap_lite.ensure_db()
    conn = _get_db()
    for row in conn.execute("SELECT * FROM movies"):
        _movies[row["movie_id"]] = dict(row)
    conn.close()
    print(f"CineMatch lite ready: {len(_movies)} movies "
          f"in {time.monotonic() - t0:.1f}s")
    yield


app = FastAPI(title="CineMatch Lite", lifespan=_lifespan)


class SimulateRequest(BaseModel):
    user_id: int
    movie_id: int
    event_type: str
    rating: float | None = None


@app.middleware("http")
async def _metrics(request: Request, call_next):
    t0 = time.monotonic()
    response = await call_next(request)
    latency = (time.monotonic() - t0) * 1000
    REQUEST_LATENCY.labels(endpoint=request.url.path).observe(latency)
    if request.url.path.startswith("/recommend"):
        _stats["requests"] += 1
        _stats["latency_ms"] = (_stats["latency_ms"] * (_stats["requests"] - 1)
                                + latency) / _stats["requests"]
    return response


@app.get("/health")
def health():
    return {"status": "ok", "mode": "lite"}


@app.get("/genres")
def genres():
    return {"genres": GENRES}


@app.get("/stats")
def stats():
    return {
        "cache_hit_rate": round(_stats["cache_hits"] / _stats["requests"], 2)
                          if _stats["requests"] else 0.0,
        "p50_latency_ms": round(_stats["latency_ms"], 1),
        "total_events":   _stats["events"],
    }


@app.get("/movies")
def list_movies(search: str | None = None, genre: str | None = None,
                page: int = Query(1, ge=1), page_size: int = Query(20, ge=1, le=60)):
    conds, args = [], []
    if genre and genre != "All":
        conds.append("genres LIKE ?")
        args.append(f"%{genre}%")
    if search:
        conds.append("title LIKE ?")
        args.append(f"%{search.strip()}%")
    where = ("WHERE " + " AND ".join(conds)) if conds else ""

    conn = _get_db()
    total = conn.execute(f"SELECT count(*) FROM movies {where}", args).fetchone()[0]
    rows = conn.execute(
        f"SELECT * FROM movies {where} ORDER BY popularity DESC, movie_id "
        f"LIMIT ? OFFSET ?", [*args, page_size, (page - 1) * page_size]).fetchall()
    conn.close()
    return {
        "movies": [dict(r) for r in rows],
        "page": page, "page_size": page_size,
        "total": total, "has_more": page * page_size < total,
    }


@app.get("/movies/{movie_id}")
def movie_detail(movie_id: int):
    conn = _get_db()
    row = conn.execute("SELECT * FROM movies WHERE movie_id = ?",
                       (movie_id,)).fetchone()
    conn.close()
    if not row:
        raise HTTPException(status_code=404, detail="Movie not found")
    return {"movie": dict(row), "similar": []}


@app.get("/recommend/{user_id}")
def recommend(user_id: int):
    variant = get_variant(user_id)
    now = datetime.now(timezone.utc).isoformat()
    conn = _get_db()

    recs = _load_user_recs(conn, user_id)             # 1. event-refreshed recs
    if recs:
        conn.close()
        CACHE_HITS.labels(result="true").inc()
        _stats["cache_hits"] += 1
        return {"user_id": user_id, "recs": recs[:10], "source": "postgres",
                "updated_at": now, "variant": variant}

    row = conn.execute("SELECT candidates FROM als_candidates WHERE user_id = ?",
                       (user_id,)).fetchone()         # 2. ALS baseline
    if row:
        recs = json.loads(row["candidates"])[:10]
        conn.close()
        CACHE_HITS.labels(result="false").inc()
        return {"user_id": user_id, "recs": [_enrich(r) for r in recs],
                "source": "als_baseline", "updated_at": now, "variant": variant}

    rows = conn.execute(                               # 3. cold-start popularity
        "SELECT movie_id, title, genres, score FROM popular ORDER BY rank LIMIT 10"
    ).fetchall()
    conn.close()
    if rows:
        return {"user_id": user_id, "recs": [dict(r) for r in rows],
                "source": "cold_start_popular", "updated_at": now,
                "variant": variant}
    return {"user_id": user_id, "recs": [], "source": "empty",
            "updated_at": now, "variant": variant}


@app.post("/simulate")
def simulate(req: SimulateRequest):
    if req.event_type not in VALID_EVENT_TYPES:
        raise HTTPException(status_code=422,
                            detail=f"event_type must be one of {VALID_EVENT_TYPES}")

    event = {
        "user_id":    req.user_id,
        "movie_id":   req.movie_id,
        "event_type": req.event_type,
        "rating":     req.rating,
        "timestamp":  datetime.now(timezone.utc).isoformat(),
    }
    _recent_events.appendleft(event)
    _stats["events"] += 1

    # Lite pipeline: no Spark re-ranking — promote the user's ALS baseline
    # into user_recs so /recommend reflects engagement immediately.
    conn = _get_db()
    row = conn.execute("SELECT candidates FROM als_candidates WHERE user_id = ?",
                       (req.user_id,)).fetchone()
    if row:
        conn.execute(
            "INSERT INTO user_recs VALUES (?,?,CURRENT_TIMESTAMP) "
            "ON CONFLICT(user_id) DO UPDATE SET recs=excluded.recs, "
            "updated_at=CURRENT_TIMESTAMP",
            (req.user_id, row["candidates"]))
        conn.commit()
    conn.close()
    return {"status": "ok", "event": event}


@app.get("/events/recent")
def recent_events():
    return {"events": list(_recent_events)[:50]}


@app.websocket("/ws/recommend/{user_id}")
async def ws_recommend(websocket: WebSocket, user_id: int):
    """Held open so the UI shows a live link; lite pushes nothing by itself."""
    await websocket.accept()
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        return


@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


if STATIC_DIR.exists():
    app.mount("/", StaticFiles(directory=str(STATIC_DIR), html=True), name="spa")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT)
