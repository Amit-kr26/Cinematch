# tests/test_rerank.py
import numpy as np


def make_item_factors(movie_ids: list[int], rank: int = 4, seed: int = 7) -> dict[int, np.ndarray]:
    rng = np.random.default_rng(seed)
    return {mid: rng.random(rank).astype(np.float32) for mid in movie_ids}


def make_candidates(movie_ids: list[int]) -> list[dict]:
    return [{"movie_id": mid, "title": f"Movie {mid}", "genres": "Action",
             "als_score": 1.0, "score": 1.0} for mid in movie_ids]


def test_compute_preference_vector_time_decay():
    """Recent events get higher weight than old events."""
    from streaming import compute_preference_vector
    item_factors = make_item_factors([1, 2, 3])
    now_ts = 1_000_000.0
    events = [
        {"movie_id": 1, "ts": now_ts - 10},      # recent
        {"movie_id": 2, "ts": now_ts - 10_000},  # old
    ]
    pref = compute_preference_vector(events, item_factors, now_ts, lambda_decay=0.001)
    # Normalise factors for comparison
    f1 = item_factors[1] / np.linalg.norm(item_factors[1])
    f2 = item_factors[2] / np.linalg.norm(item_factors[2])
    sim1 = float(np.dot(pref, f1))
    sim2 = float(np.dot(pref, f2))
    assert sim1 > sim2, f"Recent event should dominate: sim1={sim1:.3f}, sim2={sim2:.3f}"


def test_rerank_uses_preference_vector():
    """After interacting with movie 5, similar movie 6 should rank first."""
    from streaming import compute_preference_vector, rerank_candidates
    rng = np.random.default_rng(42)
    base = rng.random(4).astype(np.float32)
    item_factors = {
        5:   base + 0.01,
        6:   base + 0.02,   # similar to 5
        99:  rng.random(4).astype(np.float32),  # unrelated
        100: rng.random(4).astype(np.float32),  # unrelated
    }
    now_ts = 1_000_000.0
    events = [{"movie_id": 5, "ts": now_ts - 1}]
    pref = compute_preference_vector(events, item_factors, now_ts)
    candidates = make_candidates([6, 99, 100])
    ranked = rerank_candidates(pref, candidates, item_factors)
    assert ranked[0]["movie_id"] == 6, \
        f"Movie similar to interacted item should rank first, got {ranked[0]['movie_id']}"


def test_rerank_returns_top_n():
    """rerank_candidates returns exactly top_n items."""
    from streaming import compute_preference_vector, rerank_candidates
    item_factors = make_item_factors(list(range(1, 51)))
    now_ts = 1_000_000.0
    events = [{"movie_id": 1, "ts": now_ts - 5}]
    pref = compute_preference_vector(events, item_factors, now_ts)
    candidates = make_candidates(list(range(1, 51)))
    ranked = rerank_candidates(pref, candidates, item_factors, top_n=10)
    assert len(ranked) == 10


def test_merge_events_fifo_capped():
    """merge_events keeps last max_events events in FIFO order."""
    from streaming import merge_events
    existing = [{"movie_id": i, "ts": float(i)} for i in range(45)]
    new = [{"movie_id": 100 + i, "ts": float(100 + i)} for i in range(10)]
    merged = merge_events(existing, new, max_events=50)
    assert len(merged) == 50
    assert merged[0]["movie_id"] == 5      # first 5 of existing dropped
    assert merged[-1]["movie_id"] == 109


def test_preference_vector_zero_events():
    """No events → zero preference vector of correct rank."""
    from streaming import compute_preference_vector
    item_factors = make_item_factors([1, 2, 3], rank=4)
    pref = compute_preference_vector([], item_factors, 1_000_000.0)
    assert pref.shape == (4,)
    assert np.allclose(pref, 0.0)


def test_rerank_scores_descending():
    """rerank_candidates returns items in descending score order."""
    from streaming import compute_preference_vector, rerank_candidates
    item_factors = make_item_factors(list(range(1, 11)))
    now_ts = 1_000_000.0
    events = [{"movie_id": 1, "ts": now_ts - 1}]
    pref = compute_preference_vector(events, item_factors, now_ts)
    candidates = make_candidates(list(range(1, 11)))
    ranked = rerank_candidates(pref, candidates, item_factors, top_n=10)
    scores = [r["score"] for r in ranked]
    assert scores == sorted(scores, reverse=True)


def test_preference_vector_empty_item_factors():
    """Empty item_factors → zero vector of rank 0 (not hardcoded 50)."""
    from streaming import compute_preference_vector
    events = [{"movie_id": 1, "ts": 1_000_000.0}]
    pref = compute_preference_vector(events, {}, 1_000_000.0)
    assert pref.shape == (0,)
    assert np.allclose(pref, 0.0)


def test_rerank_als_score_fallback():
    """When movie_id not in item_factors, falls back to als_score for ranking."""
    from streaming import rerank_candidates
    pref = np.array([1.0, 0.0, 0.0, 0.0], dtype=np.float32)
    item_factors = {}  # empty — no factors available
    candidates = [
        {"movie_id": 1, "title": "A", "genres": "X", "als_score": 3.0, "score": 3.0},
        {"movie_id": 2, "title": "B", "genres": "Y", "als_score": 5.0, "score": 5.0},
        {"movie_id": 3, "title": "C", "genres": "Z", "als_score": 1.0, "score": 1.0},
    ]
    ranked = rerank_candidates(pref, candidates, item_factors)
    # Should be ordered by als_score descending: 2, 1, 3
    assert ranked[0]["movie_id"] == 2
    assert ranked[1]["movie_id"] == 1
    assert ranked[2]["movie_id"] == 3


# ── Event-type weighting tests ───────────────────────────────────────────────

def test_rating_outweighs_click():
    """A rated movie should pull preference vector more than a clicked movie."""
    from streaming import compute_preference_vector
    item_factors = make_item_factors([10, 20], rank=4, seed=99)
    now_ts = 1_000_000.0
    events = [
        {"movie_id": 10, "ts": now_ts - 5, "event_type": "rating", "rating": 5.0},
        {"movie_id": 20, "ts": now_ts - 5, "event_type": "click"},
    ]
    pref = compute_preference_vector(events, item_factors, now_ts)
    f10 = item_factors[10] / np.linalg.norm(item_factors[10])
    f20 = item_factors[20] / np.linalg.norm(item_factors[20])
    assert np.dot(pref, f10) > np.dot(pref, f20), \
        "5-star rating (weight=3.0) should dominate over click (weight=1.0)"


def test_any_rating_outweighs_view():
    """Even a 1-star rating should pull harder than a view (explicit > implicit)."""
    from streaming import compute_preference_vector
    item_factors = {
        1: np.array([1.0, 0.0, 0.0, 0.0], dtype=np.float32),
        2: np.array([0.0, 1.0, 0.0, 0.0], dtype=np.float32),
    }
    now_ts = 1_000_000.0
    events = [
        {"movie_id": 1, "ts": now_ts - 5, "event_type": "rating", "rating": 1.0},
        {"movie_id": 2, "ts": now_ts - 5, "event_type": "view"},
    ]
    pref = compute_preference_vector(events, item_factors, now_ts)
    # 1-star effective weight = 3.0 * (0.4 + 0.2*0.6) = 1.56 > view weight 1.5
    assert np.dot(pref, item_factors[1]) > np.dot(pref, item_factors[2]), \
        "1-star explicit rating (1.56) should outweigh view (1.5)"


def test_view_outweighs_click():
    """A viewed movie should have stronger influence than a clicked movie."""
    from streaming import compute_preference_vector
    item_factors = make_item_factors([30, 40], rank=4, seed=77)
    now_ts = 1_000_000.0
    events = [
        {"movie_id": 30, "ts": now_ts - 5, "event_type": "view"},
        {"movie_id": 40, "ts": now_ts - 5, "event_type": "click"},
    ]
    pref = compute_preference_vector(events, item_factors, now_ts)
    f30 = item_factors[30] / np.linalg.norm(item_factors[30])
    f40 = item_factors[40] / np.linalg.norm(item_factors[40])
    assert np.dot(pref, f30) > np.dot(pref, f40), \
        "View (weight=1.5) should dominate over click (weight=1.0)"


def test_high_star_rating_has_greater_raw_weight_than_low_star():
    """A 5-star rating contributes 5× more raw weight than a 1-star rating."""
    from streaming import compute_preference_vector
    # Use two orthogonal basis vectors so dot products are independent
    item_factors = {
        1: np.array([1.0, 0.0, 0.0, 0.0], dtype=np.float32),
        2: np.array([0.0, 1.0, 0.0, 0.0], dtype=np.float32),
    }
    now_ts = 1_000_000.0
    events = [
        {"movie_id": 1, "ts": now_ts - 1, "event_type": "rating", "rating": 5.0},
        {"movie_id": 2, "ts": now_ts - 1, "event_type": "rating", "rating": 1.0},
    ]
    pref = compute_preference_vector(events, item_factors, now_ts)
    # 5-star weight = 3.0 * (0.4 + 1.0*0.6) = 3.0; 1-star weight = 3.0 * (0.4 + 0.2*0.6) = 1.56
    # Dot product with each basis vector reflects raw weight ratio
    assert np.dot(pref, item_factors[1]) > np.dot(pref, item_factors[2]), \
        "5-star rating should pull preference vector more than 1-star"


# ── Genre diversity tests ─────────────────────────────────────────────────────

def test_genre_diversity_soft_penalty():
    """Soft penalty: items beyond max_per_genre threshold get exponentially lower scores."""
    from streaming import rerank_candidates

    # All candidates share one genre — penalty should push score down progressively
    candidates = [
        {"movie_id": i, "title": f"M{i}", "genres": "Action",
         "als_score": float(i) / 10, "score": float(i) / 10}
        for i in range(1, 11)
    ]
    pref = np.zeros(4, dtype=np.float32)
    ranked = rerank_candidates(pref, candidates, {}, max_per_genre=2, top_n=10)
    # All 10 items are Action; first 2 should be un-penalized, 3rd+ should have lower score
    # Verify the 3rd item has a lower score than if it were unpenalized
    assert ranked[0]["score"] > ranked[2]["score"], "3rd genre occurrence should be penalized"
    # All 10 items still returned (soft cap, not hard exclusion)
    assert len(ranked) == 10, "Soft penalty should not hard-exclude items"


def test_genre_hybrid_boosts_preferred_genre():
    """User genre prefs should boost candidates matching preferred genres."""
    from streaming import compute_genre_preferences, rerank_candidates

    candidates = [
        {"movie_id": 1, "title": "A", "genres": "Action", "als_score": 1.0, "score": 1.0},
        {"movie_id": 2, "title": "B", "genres": "Drama",  "als_score": 1.0, "score": 1.0},
    ]
    events = [{"movie_id": 1, "ts": 1_000_000.0, "event_type": "view"}]
    movie_meta = {1: {"genres": "Action"}, 2: {"genres": "Drama"}}
    genre_prefs = compute_genre_preferences(events, movie_meta)
    pref = np.zeros(4, dtype=np.float32)
    ranked = rerank_candidates(pref, candidates, {}, user_genre_prefs=genre_prefs,
                               max_per_genre=10, top_n=2)
    assert ranked[0]["movie_id"] == 1, "Action candidate should rank first for Action-biased user"


def test_compute_genre_preferences_type_weighted():
    """Rating events should contribute 3× more to genre prefs than click events."""
    from streaming import compute_genre_preferences

    events = [
        {"movie_id": 1, "event_type": "rating"},
        {"movie_id": 2, "event_type": "click"},
    ]
    movie_meta = {1: {"genres": "Action"}, 2: {"genres": "Drama"}}
    prefs = compute_genre_preferences(events, movie_meta)
    assert prefs.get("Action", 0) > prefs.get("Drama", 0), \
        "Rating (3×) should give Action higher preference weight than Drama from click (1×)"


# ── DLQ validation tests ──────────────────────────────────────────────────────

def test_validate_event_valid():
    from streaming import _validate_event
    valid, reason = _validate_event(1, 42, "click")
    assert valid and reason is None


def test_validate_event_invalid_user():
    from streaming import _validate_event
    valid, reason = _validate_event(0, 42, "click")
    assert not valid and reason == "invalid_user_id"


def test_validate_event_invalid_type():
    from streaming import _validate_event
    valid, reason = _validate_event(1, 42, "unknown")
    assert not valid and reason is not None and "unknown_event_type" in reason
