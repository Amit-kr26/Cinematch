# tests/test_bootstrap.py
import numpy as np
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    return (SparkSession.builder
            .master("local[2]")
            .appName("test-bootstrap")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.ui.enabled", "false")
            .getOrCreate())


@pytest.fixture(scope="session")
def tiny_ratings(spark):
    """Fixed deterministic subset — enough rows for ALS to converge."""
    rows = [
        (1, 1193, 5.0), (1, 661, 3.0), (1, 914, 3.0), (1, 3408, 4.0),
        (1, 2355, 5.0), (1, 1197, 3.0), (1, 1287, 5.0), (1, 2804, 5.0),
        (1, 594, 4.0),  (1, 919, 4.0),  (1, 595, 5.0),  (1, 938, 4.0),
        (2, 1, 4.0),    (2, 2, 3.0),    (2, 3, 5.0),    (2, 4, 2.0),
        (2, 5, 4.0),    (2, 6, 3.0),    (2, 7, 5.0),    (2, 8, 4.0),
        (3, 10, 3.0),   (3, 20, 4.0),   (3, 30, 5.0),   (3, 40, 2.0),
        (3, 50, 5.0),   (3, 60, 4.0),   (3, 1193, 4.0), (3, 594, 3.0),
    ]
    for uid in range(4, 25):
        for mid in range(uid * 10, uid * 10 + 8):
            rows.append((uid, mid, float((mid % 5) + 1)))
    return spark.createDataFrame(rows, ["userId", "movieId", "rating"])


def test_train_als_returns_model(spark, tiny_ratings):
    from bootstrap import train_als
    model = train_als(tiny_ratings, rank=5, max_iter=3, reg_param=0.1, seed=42)
    assert model is not None
    assert model.rank == 5
    assert model.itemFactors.count() > 0
    assert model.userFactors.count() > 0


def test_als_snapshot_stable(spark, tiny_ratings):
    """Same seed + same data → same top candidates for user 1."""
    from bootstrap import get_user_candidates, train_als
    model = train_als(tiny_ratings, rank=5, max_iter=3, reg_param=0.1, seed=42)
    candidates = get_user_candidates(model, tiny_ratings, user_ids=[1], n=5)
    assert 1 in candidates
    assert len(candidates[1]) == 5
    scores = [c["als_score"] for c in candidates[1]]
    assert scores == sorted(scores, reverse=True)
    # Deterministic: re-run gives identical result
    model2 = train_als(tiny_ratings, rank=5, max_iter=3, reg_param=0.1, seed=42)
    candidates2 = get_user_candidates(model2, tiny_ratings, user_ids=[1], n=5)
    assert [c["movie_id"] for c in candidates[1]] == [c["movie_id"] for c in candidates2[1]]


def test_item_similarity_shape(spark, tiny_ratings):
    """item_similarity returns ≤ top_k neighbours per item, similarity in [0,1]."""
    from bootstrap import compute_item_similarity, train_als
    model = train_als(tiny_ratings, rank=5, max_iter=3, reg_param=0.1, seed=42)
    sim = compute_item_similarity(model, top_k=3)
    for movie_id, neighbors in sim.items():
        assert len(neighbors) <= 3
        for n in neighbors:
            assert "movie_id" in n
            assert "similarity" in n
            assert 0.0 <= n["similarity"] <= 1.0 + 1e-6
        # Neighbors must be in descending similarity order
        sims = [n["similarity"] for n in neighbors]
        assert sims == sorted(sims, reverse=True)


def test_item_factors_stored(spark, tiny_ratings):
    """extract_item_factors returns {movie_id: float32 np.ndarray}."""
    from bootstrap import extract_item_factors, train_als
    model = train_als(tiny_ratings, rank=5, max_iter=3, reg_param=0.1, seed=42)
    factors = extract_item_factors(model)
    assert len(factors) > 0
    first = next(iter(factors.values()))
    assert isinstance(first, np.ndarray)
    assert first.dtype == np.float32
    assert first.shape == (5,)


def test_global_popularity_returns_sorted_top_n():
    """compute_global_popularity returns top_n items sorted by score descending."""
    import os

    from bootstrap import compute_global_popularity
    if not os.path.exists("/data/ml-1m/ratings.dat"):
        pytest.skip("MovieLens data not present")
    top20 = compute_global_popularity(top_n=20)
    assert len(top20) == 20
    scores = [x["score"] for x in top20]
    assert scores == sorted(scores, reverse=True)
    assert all("movie_id" in x and "title" in x and "genres" in x for x in top20)
