# services/api/main.py
import asyncio
import json
import logging
import os
import time
from contextlib import asynccontextmanager

import asyncpg
import httpx
import redis.asyncio as aioredis
import tmdb
from fastapi import FastAPI, Request, Response
from kafka import KafkaProducer as SyncKafkaProducer
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    REGISTRY,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)
from routers import events, health, movies, recommend, simulate, ws

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s %(message)s")

REDIS_URL     = os.getenv("REDIS_URL", "redis://redis:6379")
POSTGRES_URL  = os.getenv("POSTGRES_URL", "postgresql://recsys:recsys@postgres:5432/recsys")
KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")


def _get_or_create(cls, name: str, doc: str, *args, fallback_suffix: str = "", **kwargs):
    try:
        return cls(name, doc, *args, **kwargs)
    except ValueError:
        key = f"{name}{fallback_suffix}" if fallback_suffix else name
        return REGISTRY._names_to_collectors.get(key) or REGISTRY._names_to_collectors.get(name)


REQUEST_LATENCY = _get_or_create(
    Histogram, "api_latency_ms", "API request latency in milliseconds",
    ["method", "endpoint"],
    fallback_suffix="_bucket",
    buckets=[1, 5, 10, 25, 50, 100, 250, 500, 1000],
)
CACHE_HIT_COUNTER = _get_or_create(
    Counter, "cache_hit_total", "Redis cache hits", ["result"],
    fallback_suffix="_total",
)
DLQ_GAUGE = _get_or_create(
    Gauge, "recsys_dlq_events_total", "Total invalid events in dead-letter queue",
)
MODEL_NDCG_GAUGE = _get_or_create(
    Gauge, "recsys_model_ndcg_at_10", "ALS model NDCG@10 from last bootstrap run",
)


async def _prewarm_tmdb(app: FastAPI) -> None:
    """Background task: warm the Redis TMDB cache for the most popular movies so
    posters & descriptions render instantly on first browse/recommend. Idempotent
    (enrich skips already-cached entries), so it's cheap on every restart."""
    log = logging.getLogger("tmdb.prewarm")
    try:
        await asyncio.sleep(3)  # let startup settle
        pg, redis, http = app.state.pg_pool, app.state.redis, app.state.http
        if pg is None:
            return
        async with pg.acquire() as conn:
            rows = await conn.fetch(
                "SELECT movie_id, title, genres, year FROM movies "
                "ORDER BY popularity DESC NULLS LAST LIMIT 600"
            )
        items = [dict(r) for r in rows]
        for i in range(0, len(items), 10):  # 10 concurrent to stay under TMDB rate limit
            await tmdb.enrich(redis, http, items[i:i + 10])
            await asyncio.sleep(0.25)  # ~40 req/s TMDB free-tier limit
        log.info("Warmed TMDB cache for %d popular movies", len(items))
    except asyncio.CancelledError:
        pass
    except Exception as e:  # never let prewarm crash the app
        log.warning("TMDB prewarm skipped: %s", e)


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.redis   = aioredis.from_url(REDIS_URL, decode_responses=False)
    app.state.pg_pool = await asyncpg.create_pool(POSTGRES_URL, min_size=2, max_size=10)
    loop = asyncio.get_running_loop()
    app.state.kafka_producer = await loop.run_in_executor(
        None,
        lambda: SyncKafkaProducer(
            bootstrap_servers=KAFKA_SERVERS.split(","),
            value_serializer=lambda v: json.dumps(v).encode(),
            key_serializer=lambda k: str(k).encode(),
        )
    )
    app.state.http = httpx.AsyncClient(timeout=10)
    prewarm = asyncio.create_task(_prewarm_tmdb(app))
    yield
    prewarm.cancel()
    await app.state.http.aclose()
    await app.state.redis.aclose()
    await app.state.pg_pool.close()
    app.state.kafka_producer.close()


def create_app(lifespan_fn=None) -> FastAPI:
    """Create FastAPI app. Called both in production (with lifespan) and tests (state injected)."""
    app = FastAPI(title="Movie RecSys API", lifespan=lifespan_fn)

    app.include_router(recommend.router)
    app.include_router(simulate.router)
    app.include_router(events.router)
    app.include_router(health.router)
    app.include_router(movies.router)
    app.include_router(ws.router)

    @app.middleware("http")
    async def metrics_middleware(request: Request, call_next):
        t0 = time.monotonic()
        response = await call_next(request)
        latency_ms = (time.monotonic() - t0) * 1000
        if REQUEST_LATENCY is not None:
            REQUEST_LATENCY.labels(
                method=request.method,
                endpoint=request.url.path,
            ).observe(latency_ms)
        return response

    @app.get("/metrics")
    async def metrics(request: Request):
        # These gauges are owned by Spark/bootstrap, not the API — pull from Redis on each scrape.
        r = request.app.state.redis
        dlq_raw, ml_raw = await r.mget("dlq:count", "eval:als_metrics")
        if DLQ_GAUGE is not None:
            DLQ_GAUGE.set(int(dlq_raw or 0))
        if ml_raw and MODEL_NDCG_GAUGE is not None:
            MODEL_NDCG_GAUGE.set(json.loads(ml_raw).get("ndcg@10", 0.0))
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    @app.get("/stats")
    async def get_stats(request: Request):
        r = request.app.state.redis
        vals = await r.mget(
            "stats:cache_hits", "stats:requests_total",
            "stats:events_total", "stats:last_latency_ms", "dlq:count",
        )
        hits, total, evts = int(vals[0] or 0), int(vals[1] or 0), int(vals[2] or 0)
        latency, dlq = float(vals[3] or 0), int(vals[4] or 0)
        return {
            "cache_hit_rate": round(hits / total, 2) if total > 0 else 0.0,
            "p50_latency_ms": latency,
            "total_events":   evts,
            "dlq_count":      dlq,
        }

    @app.get("/events/dlq")
    async def dlq_events(request: Request):
        """Return up to 10 most recent invalid events routed to the dead-letter queue."""
        items = await request.app.state.redis.lrange("dlq:recent", 0, 9)
        return [json.loads(i) for i in items]

    @app.get("/stats/ml")
    async def ml_stats(request: Request):
        """Return offline ALS evaluation metrics (written by bootstrap job)."""
        raw = await request.app.state.redis.get("eval:als_metrics")
        if not raw:
            return {"error": "No evaluation data — run `make bootstrap` to generate metrics."}
        return json.loads(raw)

    @app.get("/stats/ab")
    async def ab_stats(request: Request):
        """A/B experiment: control = ALS baseline order, treatment = personalized hybrid."""
        r = request.app.state.redis
        vals = await r.mget(
            "ab:impressions:control", "ab:impressions:treatment",
            "ab:engagements:control",  "ab:engagements:treatment",
        )
        a_imp, b_imp, a_eng, b_eng = [int(v or 0) for v in vals]
        return {
            "config": {
                "control":   "ALS baseline order (no event-driven personalization)",
                "treatment": "Personalized (Spark time-decay + hybrid CF+genre)",
            },
            "control": {
                "impressions": a_imp, "engagements": a_eng,
                "engagement_rate": round(a_eng / a_imp, 4) if a_imp else 0.0,
            },
            "treatment": {
                "impressions": b_imp, "engagements": b_eng,
                "engagement_rate": round(b_eng / b_imp, 4) if b_imp else 0.0,
            },
        }

    @app.get("/stats/model-history")
    async def model_history(request: Request):
        """Return last 10 ALS training runs with eval metrics from model_runs table."""
        rows = await request.app.state.pg_pool.fetch(
            "SELECT id, trained_at, rank, max_iter, reg_param, seed, "
            "ndcg_10, precision_10, n_users_eval, is_active "
            "FROM model_runs ORDER BY trained_at DESC LIMIT 10"
        )
        return [dict(r) for r in rows]

    return app


# Production entry: app with lifespan
app = create_app(lifespan_fn=lifespan)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0",
                port=int(os.getenv("API_PORT", "8000")), reload=False)
