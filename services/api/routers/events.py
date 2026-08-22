# services/api/routers/events.py
import json
import logging

from deps import get_redis
from fastapi import APIRouter, Depends

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/events/recent")
async def recent_events(redis=Depends(get_redis)):
    raw_list = await redis.lrange("events:recent", 0, 49)
    events = [json.loads(b) for b in raw_list]
    return {"events": events}
