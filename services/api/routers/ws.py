# services/api/routers/ws.py
import json
import logging
from datetime import datetime, timezone

import tmdb
from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from routers.recommend import get_variant

router = APIRouter()
logger = logging.getLogger(__name__)


async def _build_payload(redis, http, user_id: int) -> str | None:
    """Read recs:{user_id} from Redis, enrich with TMDB, return a RecsResponse-shaped JSON string."""
    raw = await redis.get(f"recs:{user_id}")
    if not raw:
        return None
    try:
        recs = json.loads(raw)
    except (ValueError, UnicodeDecodeError):
        logger.warning("Corrupt recs in Redis for user_id=%d, skipping send", user_id)
        return None

    # Streaming job writes a bare list of recs; wrap it into the response envelope.
    if isinstance(recs, dict):
        return json.dumps(recs)

    await tmdb.enrich(redis, http, recs)
    return json.dumps({
        "user_id":    user_id,
        "recs":       recs,
        "source":     "redis",
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "variant":    get_variant(user_id),
    })


@router.websocket("/ws/recommend/{user_id}")
async def ws_recommend(websocket: WebSocket, user_id: int):
    await websocket.accept()
    redis = websocket.app.state.redis
    http = getattr(websocket.app.state, "http", None)

    payload = await _build_payload(redis, http, user_id)
    if payload:
        await websocket.send_text(payload)

    pubsub = redis.pubsub()
    await pubsub.subscribe(f"recs_update:{user_id}")
    logger.info("WS connected user_id=%d", user_id)

    try:
        async for message in pubsub.listen():
            if message["type"] != "message":
                continue
            payload = await _build_payload(redis, http, user_id)
            if payload:
                await websocket.send_text(payload)
    except WebSocketDisconnect:
        logger.info("WS disconnected user_id=%d", user_id)
    finally:
        await pubsub.unsubscribe(f"recs_update:{user_id}")
        await pubsub.aclose()
