# services/api/routers/health.py
from fastapi import APIRouter, Request

router = APIRouter()


@router.get("/health")
async def health(request: Request):
    return {"status": "ok", "mode": getattr(request.app.state, "mode", "full")}
