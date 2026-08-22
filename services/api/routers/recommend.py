# services/api/routers/recommend.py
import json
import logging
import time
from datetime import datetime, timezone
from typing import Literal, Optional

import tmdb
from deps import get_http, get_pg, get_redis
from fastapi import APIRouter, Depends, Path
from pydantic import BaseModel

router = APIRouter()
logger = logging.getLogger(__name__)


def get_variant(user_id: int, traffic_split: int = 50) -> str:
    """Deterministic hash-based bucketing — no state, reproducible."""
    return "treatment" if (user_id * 2654435761) % 100 < traffic_split else "control"


class Rec(BaseModel):
    movie_id: int
    title:    str
    genres:   str
    score:    float
    # TMDB enrichment (optional — populated by tmdb.enrich)
    poster:      Optional[str] = None
    backdrop:    Optional[str] = None
    overview:    str = ""
    year:        Optional[int] = None
    tmdb_rating: float = 0.0
    tmdb_id:     Optional[int] = None


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

    # 1. Redis (fastest path)
    raw = await redis.get(f"recs:{user_id}")
    if raw:
        recs = json.loads(raw)
        ts_raw = await redis.get(f"recs:{user_id}:ts")
        updated_at = ts_raw.decode() if ts_raw else now
        logger.info("cache_hit=true user_id=%d latency_ms=%.1f",
                    user_id, (time.monotonic() - t0) * 1000)
        await redis.incr("stats:cache_hits")
        await redis.incr("stats:requests_total")
        await redis.set("stats:last_latency_ms", f"{(time.monotonic()-t0)*1000:.1f}")
        await tmdb.enrich(redis, http, recs)
        return RecsResponse(user_id=user_id, recs=recs, source="redis",
                            updated_at=updated_at, variant=variant)

    # 2. PostgreSQL fallback (skip if recs are stale: > 30 min old)
    if pg is not None:
        async with pg.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT recs, updated_at FROM user_recs WHERE user_id = $1", user_id
            )
        if row:
            updated_at_dt = row["updated_at"]
            if updated_at_dt is not None:
                age_seconds = (datetime.now(timezone.utc)
                               - updated_at_dt.astimezone(timezone.utc)).total_seconds()
            else:
                age_seconds = 0.0  # NULL updated_at = bootstrap-seeded data, always fresh

            if age_seconds <= 1800:  # 30 min freshness threshold
                recs = json.loads(row["recs"])
                updated_at = updated_at_dt.isoformat() if updated_at_dt else now
                logger.info("cache_hit=false fallback_source=postgres user_id=%d age_s=%.0f",
                            user_id, age_seconds)
                await redis.incr("stats:requests_total")
                await tmdb.enrich(redis, http, recs)
                return RecsResponse(user_id=user_id, recs=recs, source="postgres",
                                    updated_at=updated_at, variant=variant)
            else:
                logger.info("postgres_recs_stale user_id=%d age_s=%.0f, falling through",
                            user_id, age_seconds)

    # 3. ALS baseline fallback (never empty if bootstrap ran)
    cand_raw = await redis.get(f"als_candidates:{user_id}")
    if cand_raw:
        candidates = json.loads(cand_raw)
        recs = [{"movie_id": c["movie_id"], "title": c.get("title", ""),
                 "genres": c.get("genres", ""), "score": c.get("als_score", 0.0)}
                for c in candidates[:10]]
        logger.info("cache_hit=false fallback_source=als_baseline user_id=%d", user_id)
        await redis.incr("stats:requests_total")
        await tmdb.enrich(redis, http, recs)
        return RecsResponse(user_id=user_id, recs=recs, source="als_baseline",
                            updated_at=now, variant=variant)

    # 4. Global popularity fallback (cold-start: user not in MovieLens)
    popular_raw = await redis.get("popular:global")
    if popular_raw:
        popular = json.loads(popular_raw)
        recs = [{"movie_id": c["movie_id"], "title": c.get("title", ""),
                 "genres": c.get("genres", ""), "score": c.get("score", 0.0)}
                for c in popular[:10]]
        logger.info("cold_start user_id=%d", user_id)
        await redis.incr("stats:requests_total")
        await tmdb.enrich(redis, http, recs)
        return RecsResponse(user_id=user_id, recs=recs, source="cold_start_popular",
                            updated_at=now, variant=variant)

    logger.warning("No recs found for user_id=%d", user_id)
    await redis.incr("stats:requests_total")
    return RecsResponse(user_id=user_id, recs=[], source="empty",
                        updated_at=now, variant=variant)
