# tests/test_api.py
import json
from unittest.mock import AsyncMock, MagicMock

import fakeredis
import fakeredis.aioredis
import pytest
from httpx import ASGITransport, AsyncClient

TOP10 = [
    {"movie_id": i, "title": f"Movie {i}", "genres": "Action", "score": float(10 - i)}
    for i in range(1, 11)
]


@pytest.fixture
def fake_redis():
    return fakeredis.aioredis.FakeRedis(decode_responses=False)


@pytest.fixture
def fake_redis_with_recs():
    server = fakeredis.FakeServer()
    sync_r = fakeredis.FakeRedis(server=server, decode_responses=False)
    async_r = fakeredis.aioredis.FakeRedis(server=server, decode_responses=False)
    sync_r.set("recs:1", json.dumps(TOP10).encode())
    return async_r


@pytest.fixture
async def client_redis_hit(fake_redis_with_recs):
    from main import create_app
    app = create_app()
    app.state.redis = fake_redis_with_recs
    app.state.pg_pool = None
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c


@pytest.fixture
async def client_redis_miss_pg_hit(fake_redis):
    from main import create_app
    app = create_app()
    app.state.redis = fake_redis  # empty

    mock_pool = MagicMock()
    mock_conn = AsyncMock()
    mock_conn.fetchrow = AsyncMock(return_value={"recs": json.dumps(TOP10), "updated_at": None})
    mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)
    app.state.pg_pool = mock_pool

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c


@pytest.fixture
async def client_all_miss(fake_redis):
    from main import create_app
    app = create_app()
    app.state.redis = fake_redis  # empty

    mock_pool = MagicMock()
    mock_conn = AsyncMock()
    mock_conn.fetchrow = AsyncMock(return_value=None)
    mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)
    app.state.pg_pool = mock_pool

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c


async def test_recommend_redis_hit(client_redis_hit):
    resp = await client_redis_hit.get("/recommend/1")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data["recs"]) == 10
    assert data["source"] == "redis"
    assert data["recs"][0]["movie_id"] == 1


async def test_recommend_response_schema(client_redis_hit):
    resp = await client_redis_hit.get("/recommend/1")
    rec = resp.json()["recs"][0]
    assert "movie_id" in rec
    assert "title" in rec
    assert "genres" in rec
    assert "score" in rec


async def test_recommend_postgres_fallback(client_redis_miss_pg_hit):
    resp = await client_redis_miss_pg_hit.get("/recommend/1")
    assert resp.status_code == 200
    data = resp.json()
    assert data["source"] == "postgres"
    assert len(data["recs"]) == 10


async def test_recommend_als_fallback(client_all_miss):
    """When Redis and PostgreSQL both miss, return ALS baseline from Redis als_candidates."""
    als_cands = [{"movie_id": i, "title": f"M{i}", "genres": "Drama",
                  "als_score": float(i), "score": float(i)} for i in range(1, 11)]
    await client_all_miss._transport.app.state.redis.set(
        "als_candidates:1", json.dumps(als_cands).encode()
    )
    resp = await client_all_miss.get("/recommend/1")
    assert resp.status_code == 200
    data = resp.json()
    assert data["source"] == "als_baseline"


@pytest.fixture
async def client_cold_start(fake_redis):
    """No als_candidates — but popular:global is seeded."""
    from main import create_app
    popular = [{"movie_id": i, "title": f"Pop{i}", "genres": "Drama", "score": float(10 - i)}
               for i in range(1, 11)]
    await fake_redis.set("popular:global", json.dumps(popular).encode())
    app = create_app()
    app.state.redis   = fake_redis
    app.state.pg_pool = None
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c


async def test_recommend_cold_start_popular(client_cold_start):
    """Unknown user (no als_candidates) gets global popular fallback."""
    resp = await client_cold_start.get("/recommend/99999")
    assert resp.status_code == 200
    data = resp.json()
    assert data["source"] == "cold_start_popular"
    assert len(data["recs"]) == 10


async def test_health_returns_ok(client_redis_hit):
    resp = await client_redis_hit.get("/health")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    assert "mode" in body


def test_get_variant_is_deterministic():
    from routers.recommend import get_variant
    for uid in [1, 42, 100, 9999]:
        assert get_variant(uid) == get_variant(uid)


def test_get_variant_roughly_balanced():
    from routers.recommend import get_variant
    variants = [get_variant(uid) for uid in range(200)]
    treatment_pct = sum(1 for v in variants if v == "treatment") / 200
    assert 0.35 < treatment_pct < 0.65


async def test_simulate_produces_to_kafka():
    from main import create_app
    app = create_app()
    app.state.redis = fakeredis.aioredis.FakeRedis()
    app.state.pg_pool = None

    mock_producer = MagicMock()
    app.state.kafka_producer = mock_producer

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        resp = await c.post("/simulate", json={
            "user_id": 1, "movie_id": 42, "event_type": "click"
        })
    assert resp.status_code == 200
    mock_producer.send.assert_called_once()
