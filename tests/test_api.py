# tests/test_api.py
import json
from unittest.mock import AsyncMock, MagicMock

import fakeredis
import fakeredis.aioredis
import pytest
from httpx import ASGITransport, AsyncClient
from routers.recommend import get_variant

TOP10 = [
    {"movie_id": i, "title": f"Movie {i}", "genres": "Action", "score": float(10 - i)}
    for i in range(1, 11)
]

# Distinct orderings used to prove which layer served the response.
PERSONALIZED = [  # what Spark would write to recs:{uid} after events
    {"movie_id": 100 + i, "title": f"Personalized {100 + i}", "genres": "Drama",
     "score": float(20 - i)}
    for i in range(10)
]
BASELINE = [  # static bootstrap als_candidates:{uid}
    {"movie_id": i, "title": f"Baseline {i}", "genres": "Comedy",
     "als_score": float(i), "score": float(i)}
    for i in range(1, 11)
]


def _uid_for(variant: str, start: int = 1) -> int:
    """Smallest uid >= start assigned to the given variant (hash is stable)."""
    uid = start
    while get_variant(uid) != variant:
        uid += 1
    return uid


@pytest.fixture
def fake_redis():
    return fakeredis.aioredis.FakeRedis(decode_responses=False)


@pytest.fixture
def fake_redis_with_recs():
    server = fakeredis.FakeServer()
    sync_r = fakeredis.FakeRedis(server=server, decode_responses=False)
    async_r = fakeredis.aioredis.FakeRedis(server=server, decode_responses=False)
    # user must be in the treatment bucket for the hot cache to be consulted
    sync_r.set(f"recs:{_uid_for('treatment')}", json.dumps(TOP10).encode())
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
    from routers.recommend import get_variant
    uid = _uid_for("treatment")
    resp = await client_redis_hit.get(f"/recommend/{uid}")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data["recs"]) == 10
    assert data["source"] == "redis"
    assert data["variant"] == get_variant(uid)
    assert data["recs"][0]["movie_id"] == 1


async def test_recommend_response_schema(client_redis_hit):
    resp = await client_redis_hit.get(f"/recommend/{_uid_for('treatment')}")
    rec = resp.json()["recs"][0]
    assert "movie_id" in rec
    assert "title" in rec
    assert "genres" in rec
    assert "score" in rec


async def test_recommend_postgres_fallback(client_redis_miss_pg_hit):
    """Treatment user: hot-cache miss falls through to the durable PG layer."""
    uid = _uid_for("treatment")
    resp = await client_redis_miss_pg_hit.get(f"/recommend/{uid}")
    assert resp.status_code == 200
    data = resp.json()
    assert data["variant"] == "treatment"
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


@pytest.fixture
async def client_ab(fake_redis):
    """Both variants fully provisioned: personalized cache AND static baseline."""
    from main import create_app

    control = _uid_for("control")
    treat = _uid_for("treatment")
    for uid in (control, treat):
        await fake_redis.set(f"recs:{uid}", json.dumps(PERSONALIZED).encode())
        await fake_redis.set(f"als_candidates:{uid}", json.dumps(BASELINE).encode())

    app = create_app()
    app.state.redis   = fake_redis
    app.state.pg_pool = None
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c


@pytest.fixture
async def client_control_with_pg(fake_redis):
    """Control user with BOTH a personalized Redis cache and a Spark-written
    Postgres row — neither may reach the response."""
    from main import create_app
    from routers.recommend import get_variant
    uid = next(u for u in range(1, 500) if get_variant(u) == "control")
    await fake_redis.set(f"recs:{uid}", json.dumps(PERSONALIZED).encode())
    await fake_redis.set(f"als_candidates:{uid}", json.dumps(BASELINE).encode())

    app = create_app()
    app.state.redis = fake_redis
    mock_pool = MagicMock()
    mock_conn = AsyncMock()
    mock_conn.fetchrow = AsyncMock(return_value={"recs": json.dumps(PERSONALIZED),
                                                 "updated_at": None})
    mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)
    app.state.pg_pool = mock_pool
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c, uid


async def test_ab_control_ignores_personalized_postgres(client_control_with_pg):
    client, uid = client_control_with_pg
    resp = await client.get(f"/recommend/{uid}")
    data = resp.json()
    assert data["variant"] == "control"
    assert data["source"] == "als_baseline"
    assert [r["movie_id"] for r in data["recs"]] == [b["movie_id"] for b in BASELINE]
    # the personalized PG row was never consulted
    mock_conn = client._transport.app.state.pg_pool.acquire.return_value.__aenter__
    mock_conn.assert_not_called()


async def test_ws_payload_gated_by_variant():
    """The WebSocket builder must serve control users the static ALS order,
    not the personalized cache — otherwise every browser tab leaks treatment."""
    import fakeredis.aioredis
    from routers.recommend import get_variant
    from routers.ws import _build_payload

    r = fakeredis.aioredis.FakeRedis(decode_responses=False)
    for u in range(1, 500):
        if get_variant(u) in ("control", "treatment"):
            await r.set(f"recs:{u}", json.dumps(PERSONALIZED).encode())
            await r.set(f"als_candidates:{u}", json.dumps(BASELINE).encode())
            if get_variant(u + 500) != get_variant(u):
                pass
        if len(await r.keys("als_candidates:*")) >= 2:
            break

    control = _uid_for("control")
    treat = _uid_for("treatment")

    c_payload = json.loads(await _build_payload(r, None, control, "control"))
    t_payload = json.loads(await _build_payload(r, None, treat, "treatment"))

    assert c_payload["source"] == "als_baseline"
    assert [x["movie_id"] for x in c_payload["recs"]] == [b["movie_id"] for b in BASELINE]
    assert t_payload["source"] == "redis"
    assert [x["movie_id"] for x in t_payload["recs"]] == [p["movie_id"] for p in PERSONALIZED]


async def test_ab_control_gets_baseline_not_personalized(client_ab):
    """Control must NEVER receive the Spark-personalized recs:{uid} cache —
    even when it is present and fresh — and always sees the static ALS order."""
    uid = _uid_for("control")
    resp = await client_ab.get(f"/recommend/{uid}")
    assert resp.status_code == 200
    data = resp.json()
    assert data["variant"] == "control"
    assert data["source"] == "als_baseline"
    assert [r["movie_id"] for r in data["recs"]] == [b["movie_id"] for b in BASELINE]
    # impressions still counted for the control group
    impressions = await client_ab._transport.app.state.redis.get(
        "ab:impressions:control")
    assert int(impressions) >= 1


async def test_ab_treatment_gets_personalized_cache(client_ab):
    """Treatment receives the Spark re-ranked list from the hot cache."""
    uid = _uid_for("treatment")
    resp = await client_ab.get(f"/recommend/{uid}")
    assert resp.status_code == 200
    data = resp.json()
    assert data["variant"] == "treatment"
    assert data["source"] == "redis"
    assert [r["movie_id"] for r in data["recs"]] == [p["movie_id"] for p in PERSONALIZED]


async def test_ab_groups_diverge_only_via_cache(client_ab):
    """With identical provisioning, the ONLY difference between groups is the
    personalized cache: control falls to the static layer, treatment doesn't."""
    c = (await client_ab.get(f"/recommend/{_uid_for('control')}")).json()
    t = (await client_ab.get(f"/recommend/{_uid_for('treatment')}")).json()
    assert (c["source"], t["source"]) == ("als_baseline", "redis")
    assert c["recs"][0]["movie_id"] != t["recs"][0]["movie_id"]


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


async def test_stats_ab_reports_unique_users(client_ab):
    from main import create_app
    app = create_app()
    app.state.redis = client_ab._transport.app.state.redis
    app.state.pg_pool = None
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        await c.get(f"/recommend/{_uid_for('control')}")
        resp = await c.get("/stats/ab")
    body = resp.json()
    for arm in ("control", "treatment"):
        assert {"users", "engaged_users", "engagement_rate",
                "impressions", "engagements"} <= set(body[arm].keys())
    assert body["control"]["users"] >= 1
