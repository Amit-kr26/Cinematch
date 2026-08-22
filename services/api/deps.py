# services/api/deps.py
from fastapi import Request


async def get_redis(request: Request):
    return request.app.state.redis


async def get_pg(request: Request):
    return request.app.state.pg_pool


async def get_http(request: Request):
    return getattr(request.app.state, "http", None)
