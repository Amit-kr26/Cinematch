# services/api/metrics.py
"""Shared Prometheus metric definitions."""
from prometheus_client import (
    REGISTRY,
    Counter,
    Gauge,
    Histogram,
)


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
