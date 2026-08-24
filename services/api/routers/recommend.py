# services/api/routers/recommend.py
import json
import logging
import math
import time
from datetime import datetime, timezone
from typing import Literal

import tmdb
from deps import get_http, get_pg, get_redis
from fastapi import APIRouter, Depends, Path
from metrics import CACHE_HIT_COUNTER
from pydantic import BaseModel

router = APIRouter()
logger = logging.getLogger(__name__)


def get_variant(user_id: int, traffic_split: int = 50) -> str:
    """Deterministic hash-based bucketing — no state, reproducible."""
    return "treatment" if (user_id * 2654435761) % 100 < traffic_split else "control"


def score_pct(score: float, layer: str) -> int:
    """Normalize a layer score to a 0-99 badge (linear for bounded static
    scales, logistic for signed CF dot products)."""
    if layer in ("als_baseline",):
        pct = score / 5.0 * 99.0
    elif layer == "cold_start_popular":
        pct = score / 40.0 * 99.0
    else:  # redis / postgres — unbounded signed dot products
        pct = 99.0 / (1.0 + math.exp(-score))
    return max(0, min(99, round(pct)))


class Rec(BaseModel):
    movie_id:     int
    title:        str
    genres:       str
    score:        float
    score_pct:    int | None = None
    year:         int | None = None
    poster:       str | None = None
    backdrop:     str | None = None
    overview:     str = ""
    tmdb_rating:  float = 0.0
    tmdb_id:      int | None = None


class RecsResponse(BaseModel):
    user_id:    int
    recs:       list[Rec]
    source:     Literal["redis", "postgres", "als_baseline", "cold_start_popular", "empty"]
    updated_at: str
    variant:    Literal["control", "treatment"] = "control"


@router.get("/recommend/{user_id}", response_model=RecsResponse)
async def recommend(user_id: int = Path(gt=0), redis=Depends(get_redis),
                    pg=Depends(get_pg), http=Depends(get_http)):
    t0 = time.monotonic()
    now = datetime.now(timezone.utc).isoformat()

    variant = get_variant(user_id)
    await redis.incr(f"ab:impressions:{variant}")
    await redis.sadd(f"ab:users:{variant}", user_id)

    async def serve_personalized() -> RecsResponse | None:
        raw = await redis.get(f"recs:{user_id}")
        if not raw:
            return None
        recs = json.loads(raw)
        for r in recs:
            r["score_pct"] = score_pct(r.get("score", 0.0), "redis")
        ts_raw = await redis.get(f"recs:{user_id}:ts")
        updated_at = ts_raw.decode() if ts_raw else now
        logger.info("cache_hit=true user_id=%d latency_ms=%.1f",
                    user_id, (time.monotonic() - t0) * 1000)
        CACHE_HIT_COUNTER.labels(result="true").inc()
        await redis.incr("stats:cache_hits")
        await redis.incr("stats:requests_total")
        await redis.set("stats:last_latency_ms", f"{(time.monotonic()-t0)*1000:.1f}")
        await tmdb.enrich_bounded(redis, http, recs)
        return RecsResponse(user_id=user_id, recs=recs, source="redis",
                            updated_at=updated_at, variant=variant)

    async def serve_postgres() -> RecsResponse | None:
        if pg is None:
            return None
        async with pg.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT recs, updated_at FROM user_recs WHERE user_id = $1", user_id
            )
        if not row:
            return None
        updated_at_dt = row["updated_at"]
        age_seconds = 0.0 if updated_at_dt is None else (
            datetime.now(timezone.utc)
            - updated_at_dt.astimezone(timezone.utc)).total_seconds()
        if age_seconds > 1800:
            logger.info("postgres_recs_stale user_id=%d age_s=%.0f, falling through",
                        user_id, age_seconds)
            return None
        recs = json.loads(row["recs"])
        for r in recs:
            r["score_pct"] = score_pct(r.get("score", 0.0), "postgres")
        updated_at = updated_at_dt.isoformat() if updated_at_dt else now
        logger.info("cache_hit=false fallback_source=postgres user_id=%d age_s=%.0f latency_ms=%.1f",
                    user_id, age_seconds, (time.monotonic() - t0) * 1000)
        CACHE_HIT_COUNTER.labels(result="false").inc()
        await redis.incr("stats:requests_total")
        await tmdb.enrich_bounded(redis, http, recs)
        return RecsResponse(user_id=user_id, recs=recs, source="postgres",
                            updated_at=updated_at, variant=variant)

    async def serve_als() -> RecsResponse | None:
        cand_raw = await redis.get(f"als_candidates:{user_id}")
        if not cand_raw:
            return None
        candidates = json.loads(cand_raw)
        recs = [{"movie_id": c["movie_id"], "title": c.get("title", ""),
                 "genres": c.get("genres", ""), "score": c.get("als_score", 0.0),
                 "score_pct": score_pct(c.get("als_score", 0.0), "als_baseline")}
                for c in candidates[:10]]
        logger.info("cache_hit=false fallback_source=als_baseline user_id=%d", user_id)
        CACHE_HIT_COUNTER.labels(result="false").inc()
        await redis.incr("stats:requests_total")
        await tmdb.enrich_bounded(redis, http, recs)
        return RecsResponse(user_id=user_id, recs=recs, source="als_baseline",
                            updated_at=now, variant=variant)

    async def serve_popular() -> RecsResponse | None:
        popular_raw = await redis.get("popular:global")
        if not popular_raw:
            return None
        popular = json.loads(popular_raw)
        recs = [{"movie_id": c["movie_id"], "title": c.get("title", ""),
                 "genres": c.get("genres", ""), "score": c.get("score", 0.0),
                 "score_pct": score_pct(c.get("score", 0.0), "cold_start_popular")}
                for c in popular[:10]]
        logger.info("cold_start user_id=%d", user_id)
        CACHE_HIT_COUNTER.labels(result="false").inc()
        await redis.incr("stats:requests_total")
        await tmdb.enrich_bounded(redis, http, recs)
        return RecsResponse(user_id=user_id, recs=recs, source="cold_start_popular",
                            updated_at=now, variant=variant)

    if variant == "control":
        served = await serve_als() or await serve_popular()
    else:
        served = (await serve_personalized() or await serve_postgres()
                  or await serve_als() or await serve_popular())
    if served is not None:
        return served

    logger.warning("No recs found for user_id=%d", user_id)
    CACHE_HIT_COUNTER.labels(result="false").inc()
    await redis.incr("stats:requests_total")
    return RecsResponse(user_id=user_id, recs=[], source="empty",
                        updated_at=now, variant=variant)
