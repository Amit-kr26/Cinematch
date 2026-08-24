# services/api/routers/simulate.py
import asyncio
import json
import logging
import os
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from routers.recommend import get_variant

router = APIRouter()
logger = logging.getLogger(__name__)
KAFKA_TOPIC  = os.getenv("KAFKA_TOPIC", "user-events")
RATE_LIMIT   = 10   # max events per user per second
RATE_WINDOW  = 1    # seconds


async def _check_rate_limit(redis, user_id: int) -> bool:
    """Fixed 1 s window per user; expiry armed only on first increment."""
    key = f"ratelimit:simulate:{user_id}"
    count = await redis.incr(key)
    if count == 1:
        await redis.expire(key, RATE_WINDOW)
    return count <= RATE_LIMIT


VALID_EVENT_TYPES = {"click", "view", "like", "rating"}


class SimulateRequest(BaseModel):
    user_id:    int   = Field(gt=0)
    movie_id:   int   = Field(gt=0)
    event_type: str   # "click" | "view" | "rating"
    rating:     float | None = Field(default=None, ge=1.0, le=5.0)


@router.post("/simulate")
async def simulate(req: SimulateRequest, request: Request):
    if req.event_type not in VALID_EVENT_TYPES:
        raise HTTPException(status_code=422, detail=f"event_type must be one of {VALID_EVENT_TYPES}")

    redis = request.app.state.redis
    if not await _check_rate_limit(redis, req.user_id):
        raise HTTPException(status_code=429, detail=f"Rate limit exceeded: max {RATE_LIMIT} events/sec per user")
    event: dict = {
        "user_id":    req.user_id,
        "movie_id":   req.movie_id,
        "event_type": req.event_type,
        "timestamp":  datetime.now(timezone.utc).isoformat(),
    }
    if req.rating is not None:
        event["rating"] = req.rating

    producer = request.app.state.kafka_producer
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(
        None, lambda: producer.send(KAFKA_TOPIC, key=req.user_id, value=event)
    )

    pipe = redis.pipeline()
    pipe.lpush("events:recent", json.dumps(event))
    pipe.ltrim("events:recent", 0, 49)
    pipe.incr("stats:events_total")
    await pipe.execute()

    variant = get_variant(req.user_id)
    await redis.incr(f"ab:engagements:{variant}")
    await redis.sadd(f"ab:engaged_users:{variant}", req.user_id)

    logger.info("Simulated event user_id=%d movie_id=%d type=%s",
                req.user_id, req.movie_id, req.event_type)
    return {"status": "ok", "event": event}
