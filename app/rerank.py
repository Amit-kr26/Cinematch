"""Lite re-ranking: pure-numpy port of the full stack's Spark scoring.

Same math as services/spark-jobs/streaming.py on the main branch:

    weight        = exp(-λ·age_s) × type_weight × rating_weight
      type_weight:   like=2.5  rating=3.0  view=1.5  click=1.0
      rating_weight: 0.4 + star/5 × 0.6   (any rating beats any view)
    pref          = Σ weight_i × item_factor[movie_i]   (unit-normalised)
    score         = dot(pref, item_factor) + α × genre_overlap
                    positives: s × 0.7^(c−2) / negatives: s ÷ 0.7^(c−2),
                    c = earlier same-genre titles (from 4th: ×0.70, ×0.49…)

Runs in-process on the driver-sized candidate pool (~3.7k movies) so no
Spark/Kafka is needed; recommendations still react to every event.
"""
import math

import numpy as np

LAMBDA_DECAY = 0.001
MAX_EVENTS   = 50
GENRE_HYBRID_WEIGHT = 0.15
MAX_PER_GENRE = 3

EVENT_TYPE_WEIGHTS: dict[str, float] = {
    "rating": 3.0,
    "like":   2.5,
    "view":   1.5,
    "click":  1.0,
}


def compute_preference_vector(events: list[dict],
                              factors: dict[int, np.ndarray],
                              now_ts: float) -> np.ndarray:
    """Time-decayed, event-type-weighted sum of item factors (unit vector)."""
    rank = next(iter(factors.values())).shape[0] if factors else 0
    if not events or not factors:
        return np.zeros(rank, dtype=np.float32)

    pref = np.zeros(rank, dtype=np.float32)
    for evt in events:
        factor = factors.get(evt["movie_id"])
        if factor is None:
            continue
        age         = max(0.0, now_ts - evt["ts"])
        time_weight = math.exp(-LAMBDA_DECAY * age)
        type_weight = EVENT_TYPE_WEIGHTS.get(evt.get("event_type", "click"), 1.0)
        rating_weight = 1.0
        if evt.get("event_type") == "rating" and evt.get("rating"):
            rating_weight = 0.4 + float(evt["rating"]) / 5.0 * 0.6
        pref += (time_weight * type_weight * rating_weight) * factor

    norm = np.linalg.norm(pref)
    return pref / norm if norm > 0 else pref


def genre_preferences(events: list[dict], genres_by_movie: dict[int, str]) -> dict[str, float]:
    """Normalised genre affinity from history; primary genre counts double."""
    weights: dict[str, float] = {}
    for evt in events:
        genres = [g for g in (genres_by_movie.get(evt["movie_id"]) or "").split("|") if g]
        w = EVENT_TYPE_WEIGHTS.get(evt.get("event_type", "click"), 1.0)
        for i, g in enumerate(genres):
            weights[g] = weights.get(g, 0.0) + w * (2.0 if i == 0 else 1.0)
    total = sum(weights.values())
    return {g: w / total for g, w in weights.items()} if total > 0 else weights


def rerank(pref_vector: np.ndarray,
           candidate_ids: list[int],
           factors: dict[int, np.ndarray],
           genres_by_movie: dict[int, str],
           user_genre_prefs: dict[str, float],
           baseline_scores: dict[int, float] | None = None,
           top_n: int = 10) -> list[tuple[int, float]]:
    """Score candidates by CF dot-product + genre bonus, apply diversity penalty.

    Returns [(movie_id, score)] sorted desc. Falls back to ALS baseline score
    for candidates without an item factor.
    """
    baseline_scores = baseline_scores or {}
    scored: list[tuple[int, float]] = []
    for mid in candidate_ids:
        factor = factors.get(mid)
        if factor is not None and np.linalg.norm(pref_vector) > 0:
            cf_score = float(np.dot(pref_vector, factor))
        else:
            cf_score = baseline_scores.get(mid, 0.0)
        bonus = 0.0
        if user_genre_prefs:
            gs = [g for g in (genres_by_movie.get(mid) or "").split("|") if g]
            bonus = sum(user_genre_prefs.get(g, 0.0) for g in gs) * GENRE_HYBRID_WEIGHT
        scored.append((mid, cf_score + bonus))
    scored.sort(key=lambda x: x[1], reverse=True)

    genre_counts: dict[str, int] = {}
    result: list[tuple[int, float]] = []
    for mid, score in scored:
        primary = (genres_by_movie.get(mid) or "").split("|")[0] or "Unknown"
        count = genre_counts.get(primary, 0)
        if count >= MAX_PER_GENRE:
            factor = 0.7 ** (count - MAX_PER_GENRE + 1)
            score = score * factor if score >= 0 else score / factor
        genre_counts[primary] = count + 1
        result.append((mid, round(score, 4)))
    result.sort(key=lambda x: x[1], reverse=True)
    return result[:top_n]
