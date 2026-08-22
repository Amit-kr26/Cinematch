"""CineMatch Lite — single-container demo API for HuggingFace Spaces.

Same HTTP contract as the full stack, served from one SQLite file:
  GET  /recommend/{user_id}   3-layer fallback (user_recs -> ALS -> popular)
  POST /simulate              records the event and re-ranks that user's recs
                              in-process (time-decay CF + genre hybrid, same
                              scoring math as the Spark job on main)
  GET  /movies                catalogue browse/search (paged)
  GET  /movies/{id}           detail + similar titles
  GET  /genres | /stats | /events/recent | /health | /metrics
  WS   /ws/recommend/{uid}    pushes refreshed recs immediately after events

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

import numpy as np
from fastapi import FastAPI, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import Response
from fastapi.staticfiles import StaticFiles
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    Counter,
    Histogram,
    generate_latest,
)
from pydantic import BaseModel

from app import bootstrap_lite, rerank, tmdb

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
_movies: dict[int, dict] = {}               # movie_id -> row incl. TMDB fields
_factors: dict[int, np.ndarray] = {}        # movie_id -> ALS factor vector
_popularity: dict[int, float] = {}          # Bayesian baseline score
_recent_events: deque[dict] = deque(maxlen=50)
_ws_clients: dict[int, set[WebSocket]] = {} # user_id -> live sockets
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
    out.setdefault("overview", "")
    out.setdefault("tmdb_rating", 0.0)
    return out


def _queue_art_lookup(recs: list[dict]) -> None:
    missing = [r["movie_id"] for r in recs
               if not r.get("poster") and _movies.get(r["movie_id"], {}).get("poster") is None]
    tmdb.request_enrichment(missing)


def _recs_payload(user_id: int) -> dict | None:
    """Current recs as a RecsResponse-shaped dict (used by REST + WS)."""
    conn = _get_db()
    row = conn.execute(
        "SELECT recs FROM user_recs WHERE user_id = ?", (user_id,)).fetchone()
    if not row:
        cand = conn.execute(
            "SELECT candidates FROM als_candidates WHERE user_id = ?",
            (user_id,)).fetchone()
        source = "als_baseline"
        raw = json.loads(cand["candidates"])[:10] if cand else []
    else:
        source = "postgres"
        raw = json.loads(row["recs"])
    conn.close()
    if not raw:
        return None
    recs = [_enrich(r) for r in raw][:10]
    _queue_art_lookup(recs)
    return {
        "user_id": user_id,
        "recs": recs,
        "source": source,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "variant": get_variant(user_id),
    }


def _load_user_events(conn, user_id: int) -> list[dict]:
    rows = conn.execute(
        "SELECT movie_id, event_type, rating, ts FROM events "
        "WHERE user_id = ? ORDER BY id DESC LIMIT ?",
        (user_id, rerank.MAX_EVENTS)).fetchall()
    return [{"movie_id": r["movie_id"], "event_type": r["event_type"],
             "rating": r["rating"], "ts": r["ts"]} for r in reversed(rows)]


def _rerank_user(conn, user_id: int) -> list[dict] | None:
    """Recompute this user's top-10 from their event history."""
    if not _factors:
        return None
    history = _load_user_events(conn, user_id)
    if not history:
        return None

    now_ts = time.time()
    genres_by_movie = {mid: m["genres"] for mid, m in _movies.items()}
    pref = rerank.compute_preference_vector(history, _factors, now_ts)
    g_prefs = rerank.genre_preferences(history, genres_by_movie)

    # Candidate pool: every factorised movie except ones just engaged with
    # (recommending the title you just rated back at you is noise).
    # Popularity breaks ties / feeds the blend until signal accumulates.
    engaged = {e["movie_id"] for e in history}
    candidate_ids = [mid for mid in _factors if mid not in engaged]
    ranked = rerank.rerank(pref, candidate_ids, _factors, genres_by_movie,
                           g_prefs, baseline_scores=_popularity, top_n=10)

    recs = [{"movie_id": mid,
             "title": _movies[mid]["title"] if mid in _movies else "",
             "genres": _movies[mid]["genres"] if mid in _movies else "",
             "score": score}
            for mid, score in ranked]
    conn.execute(
        "INSERT INTO user_recs VALUES (?,?,CURRENT_TIMESTAMP) "
        "ON CONFLICT(user_id) DO UPDATE SET recs=excluded.recs, "
        "updated_at=CURRENT_TIMESTAMP",
        (user_id, json.dumps(recs)))
    return recs


@asynccontextmanager
async def _lifespan(app: FastAPI):
    t0 = time.monotonic()
    bootstrap_lite.ensure_db()
    tmdb.init()
    conn = _get_db()
    for row in conn.execute("SELECT * FROM movies"):
        _movies[row["movie_id"]] = dict(row)
        _popularity[row["movie_id"]] = row["popularity"] or 0.0
    for row in conn.execute("SELECT movie_id, factor FROM item_factor"):
        _factors[row["movie_id"]] = np.frombuffer(row["factor"], dtype=np.float32).copy()
    conn.close()
    print(f"CineMatch lite ready: {len(_movies)} movies, {len(_factors)} item "
          f"factors, live TMDB={'on' if tmdb.active() else 'off'} "
          f"in {time.monotonic() - t0:.1f}s")
    yield
    await tmdb.close()


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
async def list_movies(search: str | None = None, genre: str | None = None,
                      page: int = Query(1, ge=1),
                      page_size: int = Query(20, ge=1, le=60)):
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
    movies = [dict(r) for r in rows]
    _queue_art_lookup(movies)
    return {
        "movies": movies,
        "page": page, "page_size": page_size,
        "total": total, "has_more": page * page_size < total,
    }


@app.get("/movies/{movie_id}")
async def movie_detail(movie_id: int):
    conn = _get_db()
    row = conn.execute("SELECT * FROM movies WHERE movie_id = ?",
                       (movie_id,)).fetchone()
    similar = []
    sim_raw = conn.execute("SELECT neighbors FROM item_sim WHERE movie_id = ?",
                           (movie_id,)).fetchone()
    conn.close()
    if not row:
        raise HTTPException(status_code=404, detail="Movie not found")

    if sim_raw:
        neighbors = json.loads(sim_raw["neighbors"])[:6]
        items = []
        for n in neighbors:
            meta = _movies.get(n["movie_id"])
            if meta:
                it = dict(meta)
                it["similarity"] = round(float(n.get("similarity", 0.0)), 3)
                items.append(it)
        similar = items
        _queue_art_lookup([it for it in items if not it.get("poster")])

    movie = dict(row)
    _queue_art_lookup([] if movie.get("poster") else [movie_id])
    return {"movie": movie, "similar": similar}


@app.get("/recommend/{user_id}")
async def recommend(user_id: int):
    now = datetime.now(timezone.utc).isoformat()
    variant = get_variant(user_id)

    payload = _recs_payload(user_id)                 # 1/2. personalized or ALS
    if payload:
        CACHE_HITS.labels(result="true").inc()
        _stats["cache_hits"] += 1
        return payload

    conn = _get_db()                                 # 3. cold-start popularity
    rows = conn.execute(
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
async def simulate(req: SimulateRequest):
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

    updated = None
    conn = _get_db()
    try:
        conn.execute(
            "INSERT INTO events (user_id, movie_id, event_type, rating, ts) "
            "VALUES (?,?,?,?,?)",
            (req.user_id, req.movie_id, req.event_type, req.rating, time.time()))
        updated = _rerank_user(conn, req.user_id)
        conn.commit()
    finally:
        conn.close()

    if updated is not None:                          # instant WS push
        payload = {
            "user_id": req.user_id,
            "recs": [_enrich(r) for r in updated],
            "source": "postgres",
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "variant": get_variant(req.user_id),
        }
        for ws in list(_ws_clients.get(req.user_id, ())):
            try:
                await ws.send_text(json.dumps(payload))
            except Exception:
                _ws_clients[req.user_id].discard(ws)
        _queue_art_lookup(payload["recs"])

    return {"status": "ok", "event": event, "re_ranked": updated is not None}


@app.get("/events/recent")
def recent_events():
    return {"events": list(_recent_events)[:50]}


@app.websocket("/ws/recommend/{user_id}")
async def ws_recommend(websocket: WebSocket, user_id: int):
    await websocket.accept()
    payload = _recs_payload(user_id)
    if payload:
        await websocket.send_text(json.dumps(payload))

    _ws_clients.setdefault(user_id, set()).add(websocket)
    try:
        while True:
            await websocket.receive_text()           # hold open until disconnect
    except WebSocketDisconnect:
        pass
    finally:
        _ws_clients.get(user_id, set()).discard(websocket)


@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


if STATIC_DIR.exists():
    app.mount("/", StaticFiles(directory=str(STATIC_DIR), html=True), name="spa")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT)
