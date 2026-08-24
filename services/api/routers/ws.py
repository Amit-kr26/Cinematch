# services/api/routers/ws.py
import json
import logging
from datetime import datetime, timezone

import tmdb
from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from routers.recommend import get_variant

router = APIRouter()
logger = logging.getLogger(__name__)


async def _build_payload(redis, http, user_id: int,
                         variant: str | None = None) -> str | None:
    """Build a RecsResponse-shaped JSON string honoring the A/B arm."""
    if variant is None:
        variant = get_variant(user_id)

    source = "redis"
    raw: bytes | str | None = None
    if variant == "treatment":
        raw = await redis.get(f"recs:{user_id}")

    if not raw:
        raw = await redis.get(f"als_candidates:{user_id}")
        if not raw:
            return None
        source = "als_baseline"
        candidates = json.loads(raw)
        recs = [{"movie_id": c["movie_id"], "title": c.get("title", ""),
                 "genres": c.get("genres", ""), "score": c.get("als_score", 0.0)}
                for c in candidates[:10]]
    else:
        try:
            recs = json.loads(raw)
        except (ValueError, UnicodeDecodeError):
            logger.warning("Corrupt recs in Redis for user_id=%d, skipping send",
                           user_id)
            return None

    await tmdb.enrich(redis, http, recs)
    return json.dumps({
        "user_id":    user_id,
        "recs":       recs,
        "source":     source,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "variant":    variant,
    })


@router.websocket("/ws/recommend/{user_id}")
async def ws_recommend(websocket: WebSocket, user_id: int):
    await websocket.accept()
    redis = websocket.app.state.redis
    http = getattr(websocket.app.state, "http", None)
    variant = get_variant(user_id)

    payload = await _build_payload(redis, http, user_id, variant)
    if payload:
        await websocket.send_text(payload)

    pubsub = redis.pubsub()
    await pubsub.subscribe(f"recs_update:{user_id}")
    logger.info("WS connected user_id=%d variant=%s", user_id, variant)

    try:
        async for message in pubsub.listen():
            if message["type"] != "message":
                continue
            if variant == "control":
                continue
            payload = await _build_payload(redis, http, user_id, variant)
            if payload:
                await websocket.send_text(payload)
    except WebSocketDisconnect:
        logger.info("WS disconnected user_id=%d", user_id)
    finally:
        await pubsub.unsubscribe(f"recs_update:{user_id}")
        await pubsub.aclose()
